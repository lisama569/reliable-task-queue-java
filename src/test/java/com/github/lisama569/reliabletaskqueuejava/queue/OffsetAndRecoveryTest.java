package com.github.lisama569.reliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景四：位点确认语义与跨进程重启恢复。
 * <ul>
 *   <li>只有被确认的任务才能推进位点；位点回退必须被明确拒绝；</li>
 *   <li>重启后 COMPLETED/DEAD/位点均可恢复，RUNNING（持有者随进程死亡）被重置为 PENDING 重投；</li>
 *   <li>按位点重放时，已确认任务不会被重复消费。</li>
 * </ul>
 */
class OffsetAndRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetAndRecoveryTest.class);

    private final AtomicLong now = new AtomicLong(1_000_000);

    private TaskQueue newQueue(Path dir, String file) {
        QueueConfig config = new QueueConfig(
                Duration.ofSeconds(30), 3, Duration.ofMillis(10), Duration.ofSeconds(5),
                dir.resolve(file).toString());
        return new TaskQueue(config, new JsonSnapshotStore(dir.resolve(file)), now::get);
    }

    @Test
    void offsetRequiresConfirmedTasksAndRejectsRewind(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir, "o1.json");
        queue.submit("a", null);
        queue.submit("b", null);
        queue.submit("c", null);

        // 没有任何确认时，推进到 1 必须被拒绝并指出第一个未确认任务
        QueueException.OffsetGapUnconfirmed gap = assertThrows(QueueException.OffsetGapUnconfirmed.class,
                () -> queue.commitOffset(1, "c1"));
        assertEquals("OFFSET_GAP_UNCONFIRMED", gap.getCode());
        log.info("[断言] 未确认即推进位点被拒绝 code={} msg={}", gap.getCode(), gap.getMessage());

        // 确认 id=1 后可推进到 1；跳过未确认的 id=2 直接推进到 3 仍被拒绝
        TaskRecord t1 = queue.claim("c1");
        queue.ack(t1.getId(), t1.getDeliveryTag(), "c1");
        assertEquals(1, queue.commitOffset(1, "c1"));
        assertThrows(QueueException.OffsetGapUnconfirmed.class, () -> queue.commitOffset(3, "c1"));
        log.info("[断言] key=a 确认后位点=1；跳过未确认 key=b 推进到3被拒绝");

        // 位点回退被明确拒绝（而不是重复消费已确认任务）
        QueueException.OffsetRewind rewind = assertThrows(QueueException.OffsetRewind.class,
                () -> queue.commitOffset(0, "c1"));
        assertEquals("OFFSET_REWIND_REJECTED", rewind.getCode());
        log.info("[断言] 位点回退 1 -> 0 被明确拒绝 code={} msg={}", rewind.getCode(), rewind.getMessage());

        // 连续确认后自动推进
        TaskRecord t2 = queue.claim("c1");
        queue.ack(t2.getId(), t2.getDeliveryTag(), "c1");
        assertEquals(2, queue.autoAdvanceOffset());
        log.info("[断言] key=b 确认后自动推进位点=2；key=a 已在位点之前，永不重复消费");

        // 按位点语义重放：从 0 扫描时已确认任务不会被 claim（只有未确认的 key=c 可领）
        TaskRecord only = queue.claim("c1");
        assertEquals(3, only.getId());
        assertNull(queue.claim("c1"));
        log.info("[断言] 模拟按位点重放：仅 key=c(id=3) 被投递，key=a/key=b 未重复消费");
    }

    @Test
    void restartRecoversCompletedDeadAndOffsetAndRequeuesRunning(@TempDir Path dir) {
        String file = "recover.json";

        // --- 进程 1：确认 task1，耗尽 task2 到死信，领取 task3 后“崩溃”未确认 ---
        TaskQueue q1 = newQueue(dir, file);
        q1.submit("done", null);
        q1.submit("dead", null);
        q1.submit("inflight", null);

        TaskRecord d = q1.claim("w1");
        q1.ack(d.getId(), d.getDeliveryTag(), "w1");
        q1.commitOffset(1, "w1");

        for (int i = 0; i < 3; i++) {
            if (i > 0) {
                now.addAndGet(1000); // 越过指数退避窗口
            }
            TaskRecord x = q1.claim("w1");
            q1.nack(x.getId(), x.getDeliveryTag(), "w1",
                    i < 2 ? "TRANSIENT_E" + i : "FINAL_CAUSE_E3");
        }
        TaskRecord inflight = q1.claim("w1");
        assertEquals(1, inflight.getAttempts());
        assertEquals(TaskStatus.RUNNING, q1.getTask(3).getStatus());
        log.info("[进程1] 重启前: key=done status=COMPLETED offset=1; key=dead status=DEAD lastError=FINAL_CAUSE_E3; key=inflight status=RUNNING(持有者随进程崩溃)");

        // --- 进程 2：用同一快照文件重新构建队列，模拟重启 ---
        TaskQueue q2 = newQueue(dir, file);

        assertEquals(1, q2.committedOffset(), "位点必须从快照恢复");
        assertEquals(TaskStatus.COMPLETED, q2.getTask(1).getStatus(), "已完成状态必须恢复");
        assertEquals(TaskStatus.DEAD, q2.getTask(2).getStatus(), "死信状态必须恢复");
        assertEquals("FINAL_CAUSE_E3", q2.getTask(2).getLastError(), "死信原因必须可区分地恢复");

        TaskRecord recoveredInflight = q2.getTask(3);
        assertEquals(TaskStatus.PENDING, recoveredInflight.getStatus(),
                "上个进程 RUNNING 的任务应在重启后重置为 PENDING（至少一次）");
        assertEquals(1, recoveredInflight.getAttempts(), "历史尝试次数必须保留，计入重试上限");
        log.info("[进程2] 恢复后: offset=1; key=done=COMPLETED; key=dead=DEAD({}); key=inflight: RUNNING -> PENDING(attempts保留=1)",
                q2.getTask(2).getLastError());

        // 已确认任务不重复投递；只有崩溃在途的 task3 被重新投递
        TaskRecord redelivered = q2.claim("w2");
        assertEquals(3, redelivered.getId());
        assertEquals(2, redelivered.getAttempts(), "重投是一次新的尝试");
        assertNull(q2.claim("w2"));
        log.info("[进程2] 重启后重投: key=inflight 被重新领取 id=3 attempts=2；key=done 未重复消费");

        q2.ack(redelivered.getId(), redelivered.getDeliveryTag(), "w2");

        // 即便 task3 已确认，中间的死信 task2 仍然阻塞连续位点：不会被静默跳过
        assertEquals(1, q2.autoAdvanceOffset(), "死信未显式处置前必须阻塞位点，不得静默跳过");
        log.info("[进程2] key=inflight 已确认，但 key=dead 未处置，位点仍=1（死信阻塞，未静默丢弃/跳过）");

        // 显式处置死信（留下可审计决策，原始失败原因保留）后位点才能推进到 3
        TaskRecord resolved = q2.resolveDead(2, "operator-alice", "已人工对账补偿，业务上视为完成");
        assertEquals(TaskStatus.COMPLETED, resolved.getStatus());
        assertEquals("FINAL_CAUSE_E3", resolved.getLastError());
        assertEquals(3, q2.autoAdvanceOffset());
        log.info("[进程2] key=dead 经 operator-alice 显式处置（保留原因 FINAL_CAUSE_E3）后位点推进 1 -> 3，最终状态一致");
    }
}
