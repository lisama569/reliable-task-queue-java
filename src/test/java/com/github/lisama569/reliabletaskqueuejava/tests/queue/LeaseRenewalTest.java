package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 覆盖租约续期（心跳）：长任务处理中可持续刷新占用，避免被当成失联重新投递；
 * 续期只对当前有效占用者生效，续期失败后原消费者不能再确认或上报结果。
 */
class LeaseRenewalTest {

    private static final Logger log = LoggerFactory.getLogger(LeaseRenewalTest.class);

    private static final long LEASE = 10_000L;

    @TempDir
    Path tempDir;

    private InMemoryReliableTaskQueue newQueue(TestClock clock) {
        return newQueue(clock, "snap.json");
    }

    private InMemoryReliableTaskQueue newQueue(TestClock clock, String snapshotName) {
        return QueueTestSupport.newQueue(tempDir.resolve(snapshotName), clock, 3, LEASE, 0, 1_000L, 60_000L);
    }

    @Test
    void renewExtendsLeaseAndPreventsRedelivery() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("long-task", "payload");
        TaskRecord claimed = queue.poll("worker-1");
        assertEquals(TaskState.RUNNING, claimed.getState());

        // 处理到租约快到期时续期，反复多次
        for (int i = 1; i <= 3; i++) {
            clock.advance(LEASE - 1_000L);
            AckOutcome renew = queue.renew(claimed.getOffset(), "worker-1");
            assertEquals(AckStatus.APPLIED, renew.status());
            log.info("[test] renew #{} applied, leaseExpireAt={}", i, queue.get(claimed.getOffset()).getLeaseExpireAtMillis());
            // 原租约早该过期，但续期后其他消费者仍拿不到
            assertNull(queue.poll("worker-2"), "续期后任务不应被重新投递");
            assertEquals("worker-1", queue.get(claimed.getOffset()).getOwnerToken());
        }

        // 续期后的确认仍然有效
        AckOutcome ack = queue.ack(claimed.getOffset(), "worker-1");
        assertEquals(AckStatus.APPLIED, ack.status());
        assertEquals(TaskState.SUCCEEDED, queue.get(claimed.getOffset()).getState());
    }

    @Test
    void renewRejectedForWrongOwnerAndAfterExpiry_thenOriginalOwnerCannotAck() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("task-a", "p");
        TaskRecord claimed = queue.poll("worker-1");

        // 非占用者续期被拒
        assertEquals(AckStatus.LEASE_EXPIRED, queue.renew(claimed.getOffset(), "intruder").status());

        // 租约过期后本人续期也被拒，任务被其他消费者接手
        clock.advance(LEASE + 1);
        assertEquals(AckStatus.LEASE_EXPIRED, queue.renew(claimed.getOffset(), "worker-1").status());
        TaskRecord reclaimed = queue.poll("worker-2");
        assertEquals(claimed.getOffset(), reclaimed.getOffset());
        assertEquals("worker-2", reclaimed.getOwnerToken());

        // 续期失败/被接手后，原消费者的 ack 与 nack 都必须被拒绝
        assertEquals(AckStatus.LEASE_EXPIRED, queue.ack(claimed.getOffset(), "worker-1").status());
        assertEquals(AckStatus.LEASE_EXPIRED, queue.nack(claimed.getOffset(), "worker-1", "late failure").status());
        // 新占用者可以正常确认
        assertEquals(AckStatus.APPLIED, queue.ack(claimed.getOffset(), "worker-2").status());
    }

    @Test
    void renewRejectedForTerminalAndPendingTasks() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("done-task", "p");
        queue.enqueue("pending-task", "p");

        TaskRecord done = queue.poll("w");
        queue.ack(done.getOffset(), "w");
        assertEquals(AckStatus.ALREADY_ACKED, queue.renew(done.getOffset(), "w").status());

        // PENDING（未被占用）不能续期
        TaskRecord pending = queue.get(1);
        assertEquals(TaskState.PENDING, pending.getState());
        assertEquals(AckStatus.NOT_RUNNING, queue.renew(pending.getOffset(), "w").status());
    }

    @Test
    void renewRejectedForDeadTask() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock, "dead-snap.json");
        queue.enqueue("dead-task", "p");

        // maxAttempts=3：三次投递均 nack 后进入死信
        TaskRecord first = queue.poll("w");
        queue.nack(first.getOffset(), "w", "boom-1");
        clock.advance(1_000L);
        TaskRecord second = queue.poll("w");
        queue.nack(second.getOffset(), "w", "boom-2");
        clock.advance(2_000L);
        TaskRecord third = queue.poll("w");
        queue.nack(third.getOffset(), "w", "boom-3");
        assertEquals(TaskState.DEAD, queue.get(third.getOffset()).getState());
        assertEquals(AckStatus.DEAD, queue.renew(third.getOffset(), "w").status());
    }
}
