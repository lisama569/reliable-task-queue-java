package com.github.lisama569.reliabletaskqueuejava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * 本地可靠任务队列核心服务（进程内存储 + 文件快照模拟消息代理）。
 *
 * <h3>并发模型</h3>
 * 所有状态读写都在一把内置锁内串行完成，因此“领取（抢占）/确认/失败/位点推进”
 * 相对于彼此都是原子的：同一任务任一时刻最多被一个消费者占用。
 * 每次状态变更后在锁内同步落盘快照，保证已确认的状态不丢。
 *
 * <h3>投递语义：至少一次</h3>
 * 消费者领取任务即获得一个带过期时间的租约（lease），并携带单调递增的
 * {@code deliveryTag}。租约内其他消费者无法领取；租约到期仍未确认，任务会被
 * 重新投递给其他消费者（旧持有者的 ack/nack 因 deliveryTag/持有者不匹配被拒绝）。
 * 因此重复投递可能发生，但已确认（COMPLETED）的任务永远不会被再次投递。
 *
 * <h3>幂等提交</h3>
 * 同一幂等键的重复提交返回确定的 {@link SubmitOutcome}，绝不产生第二个任务，
 * 也不会出现“既执行又报冲突”。
 *
 * <h3>位点（offset）</h3>
 * 位点表示“连续已确认完成”的高水位：只有 id 不超过目标位点的所有任务都处于
 * COMPLETED 时才允许推进；位点只能单调前进，任何回退请求都被明确拒绝。
 * 因此按位点重放不会重复消费已确认任务。
 */
public class TaskQueue {

    private static final Logger log = LoggerFactory.getLogger(TaskQueue.class);

    private final QueueConfig config;
    private final SnapshotStore store;
    private final LongSupplier clock;
    private final Object lock = new Object();

    /** id -> 记录，LinkedHashMap 保持按 id（即提交顺序）遍历。 */
    private final Map<Long, TaskRecord> byId = new LinkedHashMap<>();
    /** 幂等键 -> 记录。 */
    private final Map<String, TaskRecord> byKey = new LinkedHashMap<>();
    private long nextId = 1;
    private long committedOffset = 0;

    public TaskQueue(QueueConfig config, SnapshotStore store) {
        this(config, store, System::currentTimeMillis);
    }

    public TaskQueue(QueueConfig config, SnapshotStore store, LongSupplier clockMillis) {
        this.config = Objects.requireNonNull(config, "config");
        this.store = store;
        this.clock = Objects.requireNonNull(clockMillis, "clock");
        recover();
    }

    // ------------------------------------------------------------------
    // 恢复
    // ------------------------------------------------------------------

    private void recover() {
        QueueSnapshot snapshot = store == null ? null : store.load();
        if (snapshot == null) {
            log.info("[queue] 无历史快照，以空队列启动");
            return;
        }
        this.nextId = snapshot.getNextId();
        this.committedOffset = snapshot.getCommittedOffset();
        long recovered = 0;
        long requeued = 0;
        for (TaskRecord t : snapshot.getTasks()) {
            byId.put(t.getId(), t);
            if (t.getIdempotencyKey() != null) {
                byKey.put(t.getIdempotencyKey(), t);
            }
            recovered++;
            // 上个进程中处于 RUNNING 的任务：进程已死，其租约必然失效。
            // 为保证“至少一次”，重置为 PENDING 重新投递（attempts 保留，计入重试上限）。
            if (t.getStatus() == TaskStatus.RUNNING) {
                t.setStatus(TaskStatus.PENDING);
                t.setLeasedBy(null);
                t.setLeasedAt(0);
                t.setLeaseExpiresAt(0);
                t.setNotVisibleUntil(0);
                requeued++;
                logTransition(t, TaskStatus.RUNNING, TaskStatus.PENDING,
                        "重启恢复：租约随进程死亡而失效，重新入队待投递");
            }
        }
        log.info("[queue] 快照恢复完成: recovered={}, requeuedRunning={}, nextId={}, committedOffset={}",
                recovered, requeued, nextId, committedOffset);
        if (requeued > 0) {
            persist();
        }
    }

    // ------------------------------------------------------------------
    // 提交（幂等）
    // ------------------------------------------------------------------

    /**
     * 幂等提交任务。
     *
     * @return 确定结果：ACCEPTED（新建）/ DUPLICATE_IN_FLIGHT / ALREADY_COMPLETED / ALREADY_DEAD
     */
    public SubmitResult submit(String idempotencyKey, String payload) {
        Objects.requireNonNull(idempotencyKey, "idempotencyKey");
        synchronized (lock) {
            long now = clock.getAsLong();
            TaskRecord existing = byKey.get(idempotencyKey);
            if (existing != null) {
                SubmitOutcome outcome = switch (existing.getStatus()) {
                    case PENDING, RUNNING -> SubmitOutcome.DUPLICATE_IN_FLIGHT;
                    case COMPLETED -> SubmitOutcome.ALREADY_COMPLETED;
                    case DEAD -> SubmitOutcome.ALREADY_DEAD;
                };
                log.info("[submit] 幂等键重复提交被去重: key={}, id={}, status={}, outcome={}（不重复执行）",
                        idempotencyKey, existing.getId(), existing.getStatus(), outcome);
                return new SubmitResult(outcome, existing);
            }
            TaskRecord t = new TaskRecord();
            t.setId(nextId++);
            t.setIdempotencyKey(idempotencyKey);
            t.setPayload(payload);
            t.setStatus(TaskStatus.PENDING);
            t.setAttempts(0);
            t.setDeliveryTag(0);
            t.setCreatedAt(now);
            t.setUpdatedAt(now);
            byId.put(t.getId(), t);
            byKey.put(idempotencyKey, t);
            logTransition(t, null, TaskStatus.PENDING, "新任务入队");
            persist();
            return new SubmitResult(SubmitOutcome.ACCEPTED, t);
        }
    }

    // ------------------------------------------------------------------
    // 领取（至少一次投递 + 并发抢占）
    // ------------------------------------------------------------------

    /**
     * 领取一个可消费任务。可领取条件（按 id 顺序取第一个）：
     * <ul>
     *   <li>PENDING 且退避等待已结束；</li>
     *   <li>RUNNING 但租约已过期（崩溃/超时 → 重复投递给新消费者）。</li>
     * </ul>
     * COMPLETED/DEAD 永不再投递。无可用任务返回 null。
     */
    public TaskRecord claim(String consumerId) {
        Objects.requireNonNull(consumerId, "consumerId");
        synchronized (lock) {
            long now = clock.getAsLong();
            for (TaskRecord t : byId.values()) {
                boolean eligiblePending =
                        t.getStatus() == TaskStatus.PENDING && t.getNotVisibleUntil() <= now;
                boolean expiredLease =
                        t.getStatus() == TaskStatus.RUNNING && t.getLeaseExpiresAt() <= now;
                if (!eligiblePending && !expiredLease) {
                    continue;
                }
                TaskStatus from = t.getStatus();
                t.setStatus(TaskStatus.RUNNING);
                t.setAttempts(t.getAttempts() + 1);
                t.setDeliveryTag(t.getDeliveryTag() + 1);
                t.setLeasedBy(consumerId);
                t.setLeasedAt(now);
                t.setLeaseExpiresAt(now + config.leaseDuration().toMillis());
                t.setUpdatedAt(now);
                logTransition(t, from, TaskStatus.RUNNING,
                        "投递给消费者 " + consumerId
                                + "，deliveryTag=" + t.getDeliveryTag()
                                + "，attempts=" + t.getAttempts()
                                + "，租约到期=" + t.getLeaseExpiresAt()
                                + (from == TaskStatus.RUNNING ? "（旧租约过期，重复投递）" : ""));
                persist();
                return t;
            }
            return null;
        }
    }

    // ------------------------------------------------------------------
    // 确认 / 失败
    // ------------------------------------------------------------------

    /**
     * 确认任务完成。仅“当前租约持有者且 deliveryTag 匹配”可确认；
     * 租约已过期被他人重新领取后，旧持有者的确认被拒绝（{@link QueueException.LeaseMismatch}）。
     */
    public TaskRecord ack(long taskId, long deliveryTag, String consumerId) {
        synchronized (lock) {
            TaskRecord t = requireTask(taskId);
            TaskStatus from = t.getStatus();
            if (from != TaskStatus.RUNNING) {
                throw new QueueException.IllegalState(taskId, from, "ack");
            }
            verifyLease(t, deliveryTag, consumerId);
            t.setStatus(TaskStatus.COMPLETED);
            t.setLeasedBy(null);
            t.setLeasedAt(0);
            t.setLeaseExpiresAt(0);
            t.setNotVisibleUntil(0);
            t.setLastError(null);
            t.setCompletedAt(clock.getAsLong());
            t.setUpdatedAt(t.getCompletedAt());
            logTransition(t, from, TaskStatus.COMPLETED,
                    "消费者 " + consumerId + " 确认完成，deliveryTag=" + deliveryTag);
            persist();
            return t;
        }
    }

    /**
     * 报告消费失败。
     * <ul>
     *   <li>attempts 未达 maxAttempts：回到 PENDING，按指数退避
     *       {@code base * 2^(attempt-1)}（封顶 maxBackoff）延迟后可被再次领取；</li>
     *   <li>attempts 达到 maxAttempts：进入 DEAD 死信，保留最后失败原因，不再投递。</li>
     * </ul>
     */
    public TaskRecord nack(long taskId, long deliveryTag, String consumerId, String error) {
        synchronized (lock) {
            TaskRecord t = requireTask(taskId);
            TaskStatus from = t.getStatus();
            if (from != TaskStatus.RUNNING) {
                throw new QueueException.IllegalState(taskId, from, "nack");
            }
            verifyLease(t, deliveryTag, consumerId);
            long now = clock.getAsLong();
            t.setLastError(error);
            t.setUpdatedAt(now);
            t.setLeasedBy(null);
            t.setLeasedAt(0);
            t.setLeaseExpiresAt(0);
            if (t.getAttempts() >= config.maxAttempts()) {
                t.setStatus(TaskStatus.DEAD);
                t.setNotVisibleUntil(Long.MAX_VALUE);
                logTransition(t, from, TaskStatus.DEAD,
                        "重试耗尽（attempts=" + t.getAttempts() + "/" + config.maxAttempts()
                                + "）进入死信，最后失败原因: " + error);
            } else {
                long backoff = config.backoffMillis(t.getAttempts());
                t.setStatus(TaskStatus.PENDING);
                t.setNotVisibleUntil(now + backoff);
                logTransition(t, from, TaskStatus.PENDING,
                        "消费者 " + consumerId + " 报告失败（attempts=" + t.getAttempts()
                                + "/" + config.maxAttempts() + "），指数退避 " + backoff
                                + "ms 后重新可见，失败原因: " + error);
            }
            persist();
            return t;
        }
    }

    private void verifyLease(TaskRecord t, long deliveryTag, String consumerId) {
        if (!consumerId.equals(t.getLeasedBy()) || deliveryTag != t.getDeliveryTag()) {
            throw new QueueException.LeaseMismatch(t.getId(), consumerId);
        }
    }

    /**
     * 显式处置死信任务（人工/补偿决策）。
     *
     * <p>死信不会被静默跳过：只有显式记录处置决策后，任务才转为 COMPLETED，
     * 从而解除其对连续位点的阻塞。原始失败原因与处置决策都会保留。
     *
     * @param resolution 处置决策说明（必填，例如“已人工退款，业务上视为完成”）
     */
    public TaskRecord resolveDead(long taskId, String operator, String resolution) {
        if (resolution == null || resolution.isBlank()) {
            throw new IllegalArgumentException("resolution 必填：死信处置必须留下可审计的决策说明");
        }
        synchronized (lock) {
            TaskRecord t = requireTask(taskId);
            TaskStatus from = t.getStatus();
            if (from != TaskStatus.DEAD) {
                throw new QueueException.IllegalState(taskId, from, "resolveDead");
            }
            long now = clock.getAsLong();
            t.setStatus(TaskStatus.COMPLETED);
            t.setNotVisibleUntil(0);
            t.setCompletedAt(now);
            t.setUpdatedAt(now);
            t.setResolution("[RESOLVED by " + operator + "] " + resolution);
            logTransition(t, from, TaskStatus.COMPLETED,
                    "死信经 " + operator + " 显式处置后视为完成，决策: " + resolution
                            + "（原始失败原因保留: " + t.getLastError() + "）");
            persist();
            return t;
        }
    }

    // ------------------------------------------------------------------
    // 位点
    // ------------------------------------------------------------------

    /**
     * 显式推进提交位点。
     *
     * @throws QueueException.OffsetRewind          requested 小于当前位点（回退被明确拒绝）
     * @throws QueueException.OffsetGapUnconfirmed  目标位点之前仍有未 COMPLETED 的任务
     */
    public long commitOffset(long requestedOffset, String consumerId) {
        synchronized (lock) {
            if (requestedOffset < committedOffset) {
                throw new QueueException.OffsetRewind(requestedOffset, committedOffset);
            }
            for (TaskRecord t : byId.values()) {
                if (t.getId() > requestedOffset) {
                    break;
                }
                if (t.getStatus() != TaskStatus.COMPLETED) {
                    throw new QueueException.OffsetGapUnconfirmed(requestedOffset, t.getId());
                }
            }
            if (requestedOffset > committedOffset) {
                long from = committedOffset;
                committedOffset = requestedOffset;
                log.info("[offset] 位点推进: {} -> {}（消费者 {} 显式提交，id<=该位点的任务均已确认）",
                        from, committedOffset, consumerId);
                persist();
            }
            return committedOffset;
        }
    }

    /** 在连续已确认任务范围内自动推进位点，返回推进后的位点。 */
    public long autoAdvanceOffset() {        synchronized (lock) {
            long target = committedOffset;
            while (true) {
                TaskRecord next = byId.get(target + 1);
                if (next != null && next.getStatus() == TaskStatus.COMPLETED) {
                    target++;
                } else {
                    break;
                }
            }            if (target > committedOffset) {
                long from = committedOffset;
                committedOffset = target;
                log.info("[offset] 位点自动推进: {} -> {}（连续任务均已确认）", from, committedOffset);
                persist();
            }
            return committedOffset;
        }
    }

    public long committedOffset() {
        synchronized (lock) {
            return committedOffset;
        }
    }

    // ------------------------------------------------------------------
    // 查询
    // ------------------------------------------------------------------

    public TaskRecord getTask(long id) {
        synchronized (lock) {
            return byId.get(id);
        }
    }

    /** 按 id 顺序返回全部任务的快照视图。 */
    public List<TaskRecord> listTasks() {
        synchronized (lock) {
            return new ArrayList<>(byId.values());
        }
    }

    private TaskRecord requireTask(long id) {
        TaskRecord t = byId.get(id);
        if (t == null) {
            throw new QueueException.NotFound(id);
        }
        return t;
    }

    // ------------------------------------------------------------------
    // 持久化
    // ------------------------------------------------------------------

    private void persist() {
        if (store == null) {
            return;
        }
        QueueSnapshot snapshot = new QueueSnapshot();
        snapshot.setNextId(nextId);
        snapshot.setCommittedOffset(committedOffset);
        snapshot.setTasks(new ArrayList<>(byId.values()));
        store.save(snapshot);
    }

    private void logTransition(TaskRecord t, TaskStatus from, TaskStatus to, String detail) {
        log.info("[task] key={} id={} 状态流转: {} -> {} | {}",
                t.getIdempotencyKey(), t.getId(),
                from == null ? "-" : from, to, detail);
    }
}
