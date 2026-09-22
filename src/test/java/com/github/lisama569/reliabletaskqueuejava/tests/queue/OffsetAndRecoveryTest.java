package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.OffsetRollbackException;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖：位点回退被明确拒绝、跨越未确认任务被拒绝、
 * 进程重启后 SUCCEEDED/DEAD/PENDING/位点 可恢复、崩溃时 RUNNING 租约失效并重新投递。
 */
class OffsetAndRecoveryTest {

    private static final Logger log = LoggerFactory.getLogger(OffsetAndRecoveryTest.class);

    @TempDir
    Path tempDir;

    @Test
    void offsetRollbackAndSkippingUnconfirmedAreRejected() {
        TestClock clock = new TestClock(1_000_000L);
        Path snap = tempDir.resolve("snap.json");
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                snap, clock, 3, 30_000L, 10L, 100L);

        queue.enqueue("k0", "p0");
        queue.enqueue("k1", "p1");
        queue.enqueue("k2", "p2");

        // 确认 k0，位点自动推进到 1
        TaskRecord t0 = queue.poll("c");
        assertEquals(0, t0.getOffset());
        AckOutcome ack = queue.ack(0, "c");
        assertTrue(ack.advanced());
        assertEquals(1L, queue.committedOffset());

        // 明确拒绝位点回退
        OffsetRollbackException rb = assertThrows(OffsetRollbackException.class, () -> queue.seek(0L));
        log.info("[test] rollback rejected with message: {}", rb.getMessage());
        assertTrue(rb.getMessage().contains("rollback rejected"));
        assertEquals(1L, queue.committedOffset(), "回退被拒绝后位点保持不变，已确认任务绝不重复消费");

        // k1 尚未确认：不允许跨越
        OffsetRollbackException skip = assertThrows(OffsetRollbackException.class, () -> queue.seek(3L));
        log.info("[test] skip-unconfirmed rejected with message: {}", skip.getMessage());
        assertTrue(skip.getMessage().contains("only confirmed tasks can advance"));
        assertEquals(1L, queue.committedOffset());

        // 全部确认后位点已在 3；seek(3) 幂等无异常
        TaskRecord t1 = queue.poll("c");
        assertNotNull(t1);
        queue.ack(t1.getOffset(), "c");
        TaskRecord t2 = queue.poll("c");
        assertNotNull(t2);
        queue.ack(t2.getOffset(), "c");
        assertEquals(3L, queue.committedOffset());
        queue.seek(3L);
    }

    @Test
    void restartRecoversStatesAndOffset_andInvalidatesInFlightLeases() throws Exception {
        TestClock clock = new TestClock(1_000_000L);
        Path snap = tempDir.resolve("persist.json");
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                snap, clock, 2, 30_000L, 10L, 100L);

        // 重启前布局：
        //   offset 0 done-1    -> SUCCEEDED
        //   offset 1 dead-1    -> DEAD（保留原因）
        //   offset 2 running-1 -> RUNNING（崩溃时占用中）
        //   offset 3 pending-1 -> PENDING（running-1 占用后才入队，因此 offset 更大）
        queue.enqueue("done-1", "p");
        queue.enqueue("dead-1", "p");
        queue.enqueue("running-1", "p");

        TaskRecord t0 = queue.poll("c");
        assertEquals(0, t0.getOffset());
        queue.ack(0, "c");

        // dead-1 两次投递均失败 -> DEAD
        TaskRecord d1 = queue.poll("c");
        assertEquals(1, d1.getOffset());
        queue.nack(1, "c", "fatal: database constraint violated");
        clock.advance(11L);
        TaskRecord d2 = queue.poll("c");
        assertEquals(1, d2.getOffset());
        queue.nack(1, "c", "fatal: database constraint violated");
        assertEquals(TaskState.DEAD, queue.get(1).getState());

        // 领取 running-1（offset 2），不确认；之后再入队 pending-1（offset 3）
        TaskRecord r = queue.poll("c");
        assertEquals(2, r.getOffset());
        assertEquals(TaskState.RUNNING, queue.get(2).getState());
        String oldOwner = queue.get(2).getOwnerToken();

        queue.enqueue("pending-1", "p");
        assertEquals(TaskState.PENDING, queue.get(3).getState());
        assertEquals(1L, queue.committedOffset());
        assertTrue(Files.exists(snap), "每次状态变更后快照必须落盘");
        log.info("[test] before restart: {}", queue.listAll());

        // ===== 模拟进程重启：新队列实例 + 同一快照文件 + 新时钟（时间推进到租约早已过期之后） =====
        TestClock restartedClock = new TestClock(clock.millis() + 999_999L);
        InMemoryReliableTaskQueue restarted = QueueTestSupport.newQueue(
                snap, restartedClock, 2, 30_000L, 10L, 100L);

        assertEquals(4, restarted.listAll().size());
        assertEquals(TaskState.SUCCEEDED, restarted.get(0).getState(), "已确认状态恢复");
        assertEquals(TaskState.DEAD, restarted.get(1).getState(), "死信状态恢复");
        assertEquals("fatal: database constraint violated",
                restarted.get(1).getFailureReason(), "死信失败原因必须随快照恢复且可区分");
        assertEquals(TaskState.PENDING, restarted.get(3).getState(), "原本 PENDING 的任务保持 PENDING");
        assertEquals(1L, restarted.committedOffset(), "已提交位点恢复");

        TaskRecord recovered2 = restarted.get(2);
        assertEquals(TaskState.PENDING, recovered2.getState(),
                "重启使所有未完成租约失效：RUNNING 一律回退为 PENDING 等待重新投递（至少一次）");
        assertNull(recovered2.getOwnerToken(), "重启后不得保留任何占用者令牌");
        assertEquals(0L, recovered2.getLeaseExpireAtMillis());

        // 旧所有者令牌在重启后绝不能确认该任务
        AckOutcome staleAck = restarted.ack(2, oldOwner);
        assertEquals(AckStatus.NOT_RUNNING, staleAck.status());

        // 按序重新投递：offset 2 先于 offset 3
        TaskRecord redelivered2 = restarted.poll("new-consumer");
        assertNotNull(redelivered2);
        assertEquals(2, redelivered2.getOffset());
        log.info("[test] after restart redelivered offset=2 key='{}' attempts={} (lease had been invalidated)",
                redelivered2.getIdempotencyKey(), redelivered2.getAttempts());

        TaskRecord redelivered3 = restarted.poll("new-consumer");
        assertNotNull(redelivered3);
        assertEquals(3, redelivered3.getOffset());
        assertNull(restarted.poll("new-consumer"), "没有更多可投递任务");

        // 已成功任务不会因重启重复消费
        assertEquals(TaskState.SUCCEEDED, restarted.get(0).getState());
        assertEquals(AckStatus.ALREADY_ACKED, restarted.ack(0, "anyone").status());
        log.info("[test] after restart states={}, committedOffset={}",
                restarted.listAll(), restarted.committedOffset());
    }
}
