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
 * 覆盖主动放弃与优雅退出：未处理完的任务立即对其他消费者可见；
 * 任一时刻同一任务只有一个有效占用者；放弃后原消费者的确认被拒绝。
 */
class AbandonAndReleaseTest {

    private static final Logger log = LoggerFactory.getLogger(AbandonAndReleaseTest.class);

    private static final long LEASE = 30_000L;

    @TempDir
    Path tempDir;

    private InMemoryReliableTaskQueue newQueue(TestClock clock) {
        return QueueTestSupport.newQueue(tempDir.resolve("snap.json"), clock, 3, LEASE, 0, 1_000L, 60_000L);
    }

    @Test
    void abandonMakesTaskImmediatelyVisibleAndRejectsOriginalOwnerAck() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("task-x", "p");
        TaskRecord claimed = queue.poll("consumer-a");
        int attemptsAfterClaim = claimed.getAttempts();

        AckOutcome abandon = queue.abandon(claimed.getOffset(), "consumer-a");
        assertEquals(AckStatus.APPLIED, abandon.status());

        TaskRecord after = queue.get(claimed.getOffset());
        assertEquals(TaskState.PENDING, after.getState(), "放弃后任务必须回到 PENDING");
        assertNull(after.getOwnerToken(), "放弃后占用者必须清空");
        assertEquals(attemptsAfterClaim, after.getAttempts(), "主动放弃不额外消耗重试额度");
        assertNull(after.getFailureReason(), "主动放弃不记录失败原因");

        // 租约远未到期，但任务立刻能被其他消费者领取（无需等租约过期）
        TaskRecord reclaimed = queue.poll("consumer-b");
        assertEquals(claimed.getOffset(), reclaimed.getOffset());
        assertEquals("consumer-b", reclaimed.getOwnerToken());

        // 放弃后原消费者的 ack / nack / renew 一律被拒绝
        assertEquals(AckStatus.LEASE_EXPIRED, queue.ack(claimed.getOffset(), "consumer-a").status());
        assertEquals(AckStatus.LEASE_EXPIRED, queue.nack(claimed.getOffset(), "consumer-a", "late").status());
        assertEquals(AckStatus.LEASE_EXPIRED, queue.renew(claimed.getOffset(), "consumer-a").status());

        // 新占用者正常确认，状态唯一地落到 SUCCEEDED
        assertEquals(AckStatus.APPLIED, queue.ack(claimed.getOffset(), "consumer-b").status());
        assertEquals(TaskState.SUCCEEDED, queue.get(claimed.getOffset()).getState());
        log.info("[test] abandon flow finished, final state={}", queue.get(claimed.getOffset()).getState());
    }

    @Test
    void abandonRejectedForNonOwnerExpiredLeaseAndTerminalStates() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("task-y", "p");
        TaskRecord claimed = queue.poll("consumer-a");

        // 非占用者不能放弃别人的任务
        assertEquals(AckStatus.LEASE_EXPIRED, queue.abandon(claimed.getOffset(), "consumer-b").status());
        assertEquals("consumer-a", queue.get(claimed.getOffset()).getOwnerToken());

        // 租约过期后放弃无效（任务已可被回收）
        clock.advance(LEASE + 1);
        assertEquals(AckStatus.LEASE_EXPIRED, queue.abandon(claimed.getOffset(), "consumer-a").status());

        // 被回收并确认后，放弃返回 ALREADY_ACKED
        TaskRecord reclaimed = queue.poll("consumer-c");
        queue.ack(reclaimed.getOffset(), "consumer-c");
        assertEquals(AckStatus.ALREADY_ACKED, queue.abandon(claimed.getOffset(), "consumer-c").status());
    }

    @Test
    void releaseAllHandsOffOwnedTasksOnGracefulShutdown() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        for (int i = 0; i < 4; i++) {
            queue.enqueue("batch-" + i, "p" + i);
        }
        // consumer-a 占用 3 个，consumer-b 占用 1 个
        TaskRecord a1 = queue.poll("consumer-a");
        TaskRecord a2 = queue.poll("consumer-a");
        TaskRecord a3 = queue.poll("consumer-a");
        TaskRecord b1 = queue.poll("consumer-b");

        int released = queue.releaseAll("consumer-a");
        assertEquals(3, released);
        log.info("[test] graceful shutdown of consumer-a released {} tasks", released);

        // consumer-a 的任务全部回到 PENDING 且立即可被领取；consumer-b 的任务不受影响
        assertEquals(TaskState.PENDING, queue.get(a1.getOffset()).getState());
        assertEquals(TaskState.RUNNING, queue.get(b1.getOffset()).getState());
        assertEquals("consumer-b", queue.get(b1.getOffset()).getOwnerToken());

        TaskRecord picked = queue.poll("consumer-c");
        assertEquals(a1.getOffset(), picked.getOffset(), "释放的任务应立即可被其他消费者领取");
        // 原消费者确认被拒绝：a2 尚未被重新领取（PENDING）-> NOT_RUNNING；
        // a1 已被 consumer-c 占用（RUNNING，owner 不符）-> LEASE_EXPIRED
        assertEquals(AckStatus.NOT_RUNNING, queue.ack(a2.getOffset(), "consumer-a").status());
        assertEquals(AckStatus.LEASE_EXPIRED, queue.ack(a1.getOffset(), "consumer-a").status());

        // 再次释放返回 0，且空令牌被拒
        assertEquals(0, queue.releaseAll("consumer-a"));
    }

    @Test
    void stateRemainsUniqueAcrossClaimAbandonAndReclaimCycle() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = newQueue(clock);
        queue.enqueue("cycle-task", "p");

        // 正常处理 -> 放弃重投 -> 失联回收，三种流转中状态始终唯一可解释
        TaskRecord c1 = queue.poll("worker-1");
        assertEquals(TaskState.RUNNING, queue.get(c1.getOffset()).getState());
        queue.abandon(c1.getOffset(), "worker-1");
        assertEquals(TaskState.PENDING, queue.get(c1.getOffset()).getState());

        TaskRecord c2 = queue.poll("worker-2");
        assertEquals(TaskState.RUNNING, queue.get(c2.getOffset()).getState());
        clock.advance(LEASE + 1); // worker-2 失联，租约过期
        TaskRecord c3 = queue.poll("worker-3");
        assertEquals(c2.getOffset(), c3.getOffset(), "失联回收后应重新投递同一任务");
        assertEquals("worker-3", queue.get(c3.getOffset()).getOwnerToken());

        assertEquals(AckStatus.APPLIED, queue.ack(c3.getOffset(), "worker-3").status());
        assertEquals(TaskState.SUCCEEDED, queue.get(c3.getOffset()).getState());
        // 终态后任何占用操作都不再有效
        assertEquals(AckStatus.ALREADY_ACKED, queue.abandon(c3.getOffset(), "worker-3").status());
        assertNull(queue.poll("worker-4"));
    }
}
