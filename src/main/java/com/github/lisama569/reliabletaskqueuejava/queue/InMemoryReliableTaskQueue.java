package com.github.lisama569.reliabletaskqueuejava.queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 进程内可靠任务队列实现。
 *
 * <p>单一 {@link ReentrantLock} 保护全部状态变更并在变更后同步快照，
 * 保证并发抢占的确定性与重启恢复的一致性。
 *
 * <h2>投递语义</h2>
 * <ul>
 *   <li>至少一次：失败 nack 或租约过期后任务可被再次投递；</li>
 *   <li>单占用：任务只在 PENDING（或 RUNNING 且租约过期）时可被领取，
 *       领取后写入 ownerToken 与租约到期时间，其他消费者在租约内无法抢占；</li>
 *   <li>幂等：相同幂等键完成前去重（DEDUPLICATED），已成功返回 ALREADY_SUCCEEDED，
 *       已死信返回 ALREADY_DEAD——任何情况下都不会再次执行。</li>
 * </ul>
 */
public class InMemoryReliableTaskQueue implements TaskQueue {

    private static final Logger log = LoggerFactory.getLogger(InMemoryReliableTaskQueue.class);

    private final int maxAttempts;
    private final long leaseMillis;
    private final int capacity;
    private final RetryPolicy retryPolicy;
    private final TaskQueueStore store;
    private final Clock clock;

    private final ReentrantLock lock = new ReentrantLock(true);
    private final Map<String, TaskRecord> byKey = new LinkedHashMap<>();
    private final List<TaskRecord> tasks = new ArrayList<>();
    private long nextOffset = 0L;
    private long committedOffset = 0L;

    public InMemoryReliableTaskQueue(int maxAttempts,
                                     long leaseMillis,
                                     RetryPolicy retryPolicy,
                                     TaskQueueStore store,
                                     Clock clock) {
        this(maxAttempts, leaseMillis, 0, retryPolicy, store, clock);
    }

    public InMemoryReliableTaskQueue(int maxAttempts,
                                     long leaseMillis,
                                     int capacity,
                                     RetryPolicy retryPolicy,
                                     TaskQueueStore store,
                                     Clock clock) {
        this.maxAttempts = maxAttempts;
        this.leaseMillis = leaseMillis;
        this.capacity = capacity;
        this.retryPolicy = retryPolicy;
        this.store = store;
        this.clock = clock;
    }

    /**
     * 从持久化快照恢复：重建索引；重启前处于 RUNNING 的任务租约一律视为失效，
     * 立即重置为 PENDING 等待重新投递（至少一次），不依赖崩溃前的消费者心跳。
     */
    public void recover() {
        lock.lock();
        try {
            QueueSnapshot snapshot = store.load();
            if (snapshot == null || snapshot.getTasks() == null) {
                return;
            }
            tasks.clear();
            byKey.clear();
            long maxOffset = -1L;
            for (TaskRecord t : snapshot.getTasks()) {
                tasks.add(t);
                byKey.put(t.getIdempotencyKey(), t);
                maxOffset = Math.max(maxOffset, t.getOffset());
                if (t.getState() == TaskState.RUNNING) {
                    t.setState(TaskState.PENDING);
                    t.setOwnerToken(null);
                    t.setLeaseExpireAtMillis(0L);
                    t.setNextVisibleAtMillis(clock.millis());
                    t.setPendingReason(PendingReason.LEASE_RECOVERED);
                    log.info("recover: key='{}' offset={} RUNNING -> PENDING reason=LEASE_RECOVERED (lease invalidated by restart)",
                            t.getIdempotencyKey(), t.getOffset());
                } else {
                    log.info("recover: key='{}' offset={} restored as {} pendingReason={}",
                            t.getIdempotencyKey(), t.getOffset(), t.getState(), t.getPendingReason());
                }
            }
            nextOffset = maxOffset + 1;
            committedOffset = Math.min(snapshot.getCommittedOffset(), nextOffset);
            log.info("recover complete: tasks={}, nextOffset={}, committedOffset={}",
                    tasks.size(), nextOffset, committedOffset);
            persistLocked();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public EnqueueResult enqueue(String idempotencyKey, String payload) {
        if (idempotencyKey == null || idempotencyKey.isBlank()) {
            throw new IllegalArgumentException("idempotencyKey must not be blank");
        }
        lock.lock();
        try {
            TaskRecord existing = byKey.get(idempotencyKey);
            if (existing != null) {
                EnqueueStatus status = switch (existing.getState()) {
                    case PENDING, RUNNING -> EnqueueStatus.DEDUPLICATED;
                    case SUCCEEDED -> EnqueueStatus.ALREADY_SUCCEEDED;
                    case DEAD -> EnqueueStatus.ALREADY_DEAD;
                };
                log.info("enqueue key='{}' -> {} (offset={}, currentState={}): duplicate submit does not execute again",
                        idempotencyKey, status, existing.getOffset(), existing.getState());
                return new EnqueueResult(status, existing);
            }
            // 容量背压：幂等检查优先——重复提交（含已完成/已死信）走到上面直接返回，
            // 绝不额外占用容量，也不会因容量限制改变其幂等结果。
            if (capacity > 0 && tasks.size() >= capacity) {
                log.info("enqueue key='{}' -> QUEUE_FULL currentSize={} capacity={} (backpressure: task retained nowhere, caller should retry later)",
                        idempotencyKey, tasks.size(), capacity);
                return new EnqueueResult(EnqueueStatus.QUEUE_FULL, null);
            }
            long now = clock.millis();
            TaskRecord t = new TaskRecord();
            t.setOffset(nextOffset++);
            t.setIdempotencyKey(idempotencyKey);
            t.setPayload(payload);
            t.setState(TaskState.PENDING);
            t.setAttempts(0);
            t.setMaxAttempts(maxAttempts);
            t.setEnqueueAtMillis(now);
            t.setNextVisibleAtMillis(now);
            t.setPendingReason(PendingReason.ENQUEUED);
            tasks.add(t);
            byKey.put(idempotencyKey, t);
            persistLocked();
            log.info("transition key='{}' offset={} -> PENDING (enqueued)", idempotencyKey, t.getOffset());
            return new EnqueueResult(EnqueueStatus.ACCEPTED, t);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TaskRecord poll(String ownerToken) {
        if (ownerToken == null || ownerToken.isBlank()) {
            throw new IllegalArgumentException("ownerToken must not be blank");
        }
        lock.lock();
        try {
            long now = clock.millis();
            for (TaskRecord t : tasks) {
                if (t.getState() == TaskState.PENDING && t.getNextVisibleAtMillis() <= now) {
                    return claimLocked(t, ownerToken, now, false);
                }
                if (t.getState() == TaskState.RUNNING && t.getLeaseExpireAtMillis() <= now) {
                    log.info("lease expired: key='{}' offset={} previousOwner='{}' -> redelivered to '{}'",
                            t.getIdempotencyKey(), t.getOffset(), t.getOwnerToken(), ownerToken);
                    t.setPendingReason(PendingReason.LEASE_RECOVERED);
                    return claimLocked(t, ownerToken, now, true);
                }
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    private TaskRecord claimLocked(TaskRecord t, String ownerToken, long now, boolean redelivery) {
        PendingReason previousPendingReason = t.getPendingReason();
        t.setState(TaskState.RUNNING);
        t.setOwnerToken(ownerToken);
        t.setLeaseExpireAtMillis(now + leaseMillis);
        t.setAttempts(t.getAttempts() + 1);
        t.setPendingReason(null);
        persistLocked();
        log.info("transition key='{}' offset={} -> RUNNING owner='{}' attempt={}/{} redelivery={} pendingReason={}",
                t.getIdempotencyKey(), t.getOffset(), ownerToken,
                t.getAttempts(), t.getMaxAttempts(), redelivery,
                redelivery ? previousPendingReason : null);
        return t;
    }

    @Override
    public AckOutcome ack(long offset, String ownerToken) {
        lock.lock();
        try {
            TaskRecord t = taskAt(offset);
            if (t == null) {
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            if (t.getState() == TaskState.SUCCEEDED) {
                log.info("ack key='{}' offset={} -> ALREADY_ACKED: committed result retained, no re-consume",
                        t.getIdempotencyKey(), offset);
                return new AckOutcome(AckStatus.ALREADY_ACKED, false);
            }
            if (t.getState() == TaskState.DEAD) {
                return new AckOutcome(AckStatus.DEAD, false);
            }
            if (t.getState() == TaskState.PENDING) {
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            if (!ownerToken.equals(t.getOwnerToken()) || clock.millis() > t.getLeaseExpireAtMillis()) {
                log.info("ack rejected: key='{}' offset={} owner='{}' leaseExpireAt={} -> LEASE_EXPIRED",
                        t.getIdempotencyKey(), offset, ownerToken, t.getLeaseExpireAtMillis());
                return new AckOutcome(AckStatus.LEASE_EXPIRED, false);
            }
            t.setState(TaskState.SUCCEEDED);
            t.setOwnerToken(null);
            t.setLeaseExpireAtMillis(0L);
            t.setCompletedAtMillis(clock.millis());
            boolean advanced = advanceCommittedOffsetLocked();
            persistLocked();
            log.info("transition key='{}' offset={} RUNNING -> SUCCEEDED owner='{}', committedOffset={}",
                    t.getIdempotencyKey(), offset, ownerToken, committedOffset);
            return new AckOutcome(AckStatus.APPLIED, advanced);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AckOutcome nack(long offset, String ownerToken, String reason) {
        lock.lock();
        try {
            TaskRecord t = taskAt(offset);
            if (t == null) {
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            if (t.getState() == TaskState.SUCCEEDED) {
                return new AckOutcome(AckStatus.ALREADY_ACKED, false);
            }
            if (t.getState() == TaskState.DEAD) {
                return new AckOutcome(AckStatus.DEAD, false);
            }
            if (t.getState() == TaskState.PENDING) {
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            if (!ownerToken.equals(t.getOwnerToken()) || clock.millis() > t.getLeaseExpireAtMillis()) {
                log.info("nack rejected: key='{}' offset={} owner='{}' -> LEASE_EXPIRED",
                        t.getIdempotencyKey(), offset, ownerToken);
                return new AckOutcome(AckStatus.LEASE_EXPIRED, false);
            }
            t.setOwnerToken(null);
            t.setLeaseExpireAtMillis(0L);
            if (t.getAttempts() >= t.getMaxAttempts()) {
                t.setState(TaskState.DEAD);
                t.setFailureReason(reason);
                t.setPendingReason(null);
                t.setCompletedAtMillis(clock.millis());
                persistLocked();
                log.info("transition key='{}' offset={} RUNNING -> DEAD attempts={}/{} reason='{}' (retained in dead-letter queue)",
                        t.getIdempotencyKey(), offset, t.getAttempts(), t.getMaxAttempts(), reason);
                return new AckOutcome(AckStatus.APPLIED, false);
            }
            long backoff = retryPolicy.backoffMillis(t.getAttempts());
            t.setState(TaskState.PENDING);
            t.setFailureReason(reason);
            t.setPendingReason(PendingReason.RETRY_BACKOFF);
            t.setNextVisibleAtMillis(clock.millis() + backoff);
            persistLocked();
            log.info("transition key='{}' offset={} RUNNING -> PENDING retryScheduledInMs={} attempts={}/{} lastReason='{}'",
                    t.getIdempotencyKey(), offset, backoff, t.getAttempts(), t.getMaxAttempts(), reason);
            return new AckOutcome(AckStatus.APPLIED, false);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public RenewOutcome renew(long offset, String ownerToken) {
        if (ownerToken == null || ownerToken.isBlank()) {
            throw new IllegalArgumentException("ownerToken must not be blank");
        }
        lock.lock();
        try {
            TaskRecord t = taskAt(offset);
            if (t == null) {
                return new RenewOutcome(RenewStatus.NOT_RUNNING, 0L);
            }
            if (t.getState() == TaskState.SUCCEEDED) {
                return new RenewOutcome(RenewStatus.ALREADY_ACKED, 0L);
            }
            if (t.getState() == TaskState.DEAD) {
                return new RenewOutcome(RenewStatus.DEAD, 0L);
            }
            if (t.getState() == TaskState.PENDING) {
                log.info("renew rejected: key='{}' offset={} owner='{}' -> NOT_RUNNING",
                        t.getIdempotencyKey(), offset, ownerToken);
                return new RenewOutcome(RenewStatus.NOT_RUNNING, 0L);
            }
            long now = clock.millis();
            // 仅当前有效占用者（令牌匹配且租约未过期、未被他人接手）可续期
            if (!ownerToken.equals(t.getOwnerToken()) || now > t.getLeaseExpireAtMillis()) {
                log.info("renew rejected: key='{}' offset={} owner='{}' currentOwner='{}' leaseExpireAt={} now={} -> LEASE_EXPIRED",
                        t.getIdempotencyKey(), offset, ownerToken, t.getOwnerToken(),
                        t.getLeaseExpireAtMillis(), now);
                return new RenewOutcome(RenewStatus.LEASE_EXPIRED, 0L);
            }
            long newExpireAt = now + leaseMillis;
            t.setLeaseExpireAtMillis(newExpireAt);
            persistLocked();
            log.info("renew key='{}' offset={} owner='{}' leaseExpireAt={} -> {} (heartbeat extended)",
                    t.getIdempotencyKey(), offset, ownerToken, newExpireAt - leaseMillis, newExpireAt);
            return new RenewOutcome(RenewStatus.RENEWED, newExpireAt);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public AckOutcome abandon(long offset, String ownerToken, String reason) {
        if (ownerToken == null || ownerToken.isBlank()) {
            throw new IllegalArgumentException("ownerToken must not be blank");
        }
        lock.lock();
        try {
            TaskRecord t = taskAt(offset);
            if (t == null) {
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            if (t.getState() == TaskState.SUCCEEDED) {
                return new AckOutcome(AckStatus.ALREADY_ACKED, false);
            }
            if (t.getState() == TaskState.DEAD) {
                return new AckOutcome(AckStatus.DEAD, false);
            }
            if (t.getState() == TaskState.PENDING) {
                log.info("abandon rejected: key='{}' offset={} owner='{}' -> NOT_RUNNING",
                        t.getIdempotencyKey(), offset, ownerToken);
                return new AckOutcome(AckStatus.NOT_RUNNING, false);
            }
            // 仅当前有效占用者可放弃；租约已过期/已被他人接手则拒绝，
            // 与 ack/nack 使用完全相同的有效性判定，放弃后不可能再确认
            if (!ownerToken.equals(t.getOwnerToken()) || clock.millis() > t.getLeaseExpireAtMillis()) {
                log.info("abandon rejected: key='{}' offset={} owner='{}' currentOwner='{}' -> LEASE_EXPIRED",
                        t.getIdempotencyKey(), offset, ownerToken, t.getOwnerToken());
                return new AckOutcome(AckStatus.LEASE_EXPIRED, false);
            }
            long now = clock.millis();
            t.setState(TaskState.PENDING);
            t.setOwnerToken(null);
            t.setLeaseExpireAtMillis(0L);
            t.setNextVisibleAtMillis(now);
            t.setPendingReason(PendingReason.ABANDONED);
            persistLocked();
            log.info("transition key='{}' offset={} RUNNING -> PENDING reason=ABANDONED owner='{}' lastReason='{}' "
                            + "(immediately reclaimable by other consumers)",
                    t.getIdempotencyKey(), offset, ownerToken, reason);
            return new AckOutcome(AckStatus.APPLIED, false);
        } finally {
            lock.unlock();
        }
    }

    /**
     * 位点只沿连续的 SUCCEEDED 前缀向前推进；空洞（未完成任务）会挡住位点。
     */
    private boolean advanceCommittedOffsetLocked() {
        long before = committedOffset;
        while (committedOffset < tasks.size()) {
            TaskRecord t = tasks.get((int) committedOffset);
            if (t.getState() != TaskState.SUCCEEDED) {
                break;
            }
            committedOffset++;
        }
        return committedOffset != before;
    }

    @Override
    public long committedOffset() {
        lock.lock();
        try {
            return committedOffset;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void seek(long targetOffsetInclusive) {
        lock.lock();
        try {
            if (targetOffsetInclusive < committedOffset) {
                throw new OffsetRollbackException(
                        "offset rollback rejected: requested=" + targetOffsetInclusive
                                + ", currentCommitted=" + committedOffset
                                + " (confirmed tasks must never be consumed again)");
            }
            if (targetOffsetInclusive > tasks.size()) {
                throw new OffsetRollbackException(
                        "seek rejected: requested=" + targetOffsetInclusive
                                + " beyond highest offset " + tasks.size());
            }
            for (long o = committedOffset; o < targetOffsetInclusive; o++) {
                TaskRecord t = tasks.get((int) o);
                if (t.getState() != TaskState.SUCCEEDED) {
                    throw new OffsetRollbackException(
                            "seek rejected: task at offset " + o + " (key='" + t.getIdempotencyKey()
                                    + "') is " + t.getState() + ", only confirmed tasks can advance the offset");
                }
            }
            long before = committedOffset;
            committedOffset = targetOffsetInclusive;
            persistLocked();
            log.info("seek: committedOffset {} -> {}", before, committedOffset);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public TaskRecord get(long offset) {
        lock.lock();
        try {
            return taskAt(offset);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TaskRecord> listAll() {
        lock.lock();
        try {
            return new ArrayList<>(tasks);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public List<TaskRecord> listDeadLetters() {
        lock.lock();
        try {
            return tasks.stream().filter(t -> t.getState() == TaskState.DEAD).toList();
        } finally {
            lock.unlock();
        }
    }

    @Override
    public int maxAttempts() {
        return maxAttempts;
    }

    @Override
    public int capacity() {
        return capacity;
    }

    @Override
    public int currentSize() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }

    private TaskRecord taskAt(long offset) {
        if (offset < 0 || offset >= tasks.size()) {
            return null;
        }
        return tasks.get((int) offset);
    }

    private void persistLocked() {
        QueueSnapshot snapshot = new QueueSnapshot();
        snapshot.setCommittedOffset(committedOffset);
        snapshot.setTasks(new ArrayList<>(tasks));
        store.save(snapshot);
    }
}
