package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueResult;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 覆盖：未完成期间重复提交去重、完成后重复提交的确定结果、
 * 租约过期导致的重复投递（至少一次）、重复 ack 无害。
 */
class IdempotencyAndDeliveryTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotencyAndDeliveryTest.class);

    @TempDir
    Path tempDir;

    @Test
    void duplicateSubmitBeforeCompletion_isDeduplicatedAndNeverExecutedTwice() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "order-1001";
        EnqueueResult first = queue.enqueue(key, "pay");
        EnqueueResult second = queue.enqueue(key, "pay-again");
        EnqueueResult third = queue.enqueue(key, "pay-again-2");
        log.info("[test] key='{}' submit outcomes: {}, {}, {}",
                key, first.status(), second.status(), third.status());

        assertEquals(EnqueueStatus.ACCEPTED, first.status());
        assertEquals(EnqueueStatus.DEDUPLICATED, second.status());
        assertEquals(EnqueueStatus.DEDUPLICATED, third.status());
        assertSame(first.task(), second.task(), "重复提交必须返回同一个任务实体，绝不产生新任务");
        assertEquals(1, queue.listAll().size());

        AtomicInteger executions = new AtomicInteger();
        TaskRecord claimed = queue.poll("consumer-A");
        if (claimed != null) {
            executions.incrementAndGet();
        }
        TaskRecord stolen = queue.poll("consumer-B");
        log.info("[test] key='{}' firstPoll={}, secondPollInLease={}",
                key, claimed == null ? null : claimed.getState(), stolen);

        assertNotNull(claimed);
        assertNull(stolen, "租约期内同一任务不得被第二个消费者占用");
        assertEquals(1, executions.get(), "去重期间任务只能被执行一次");
    }

    @Test
    void resubmitAfterSucceeded_returnsAlreadySucceededAndDoesNotReexecute() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "order-1002";
        queue.enqueue(key, "ship");
        TaskRecord t = queue.poll("c1");
        AckOutcome ack = queue.ack(t.getOffset(), "c1");
        assertEquals(AckStatus.APPLIED, ack.status());
        assertEquals(TaskState.SUCCEEDED, t.getState());
        log.info("[test] key='{}' completed, committedOffset={}", key, queue.committedOffset());

        // 已完成后的再次提交：确定且可解释——返回原任务与 ALREADY_SUCCEEDED，不会再次执行
        EnqueueResult resubmit = queue.enqueue(key, "ship-v2");
        log.info("[test] key='{}' resubmit after success -> {}", key, resubmit.status());
        assertEquals(EnqueueStatus.ALREADY_SUCCEEDED, resubmit.status());
        assertSame(t, resubmit.task());
        assertEquals(1, queue.listAll().size());
        assertNull(queue.poll("c1"), "已成功任务不能被再次投递执行");

        AckOutcome duplicateAck = queue.ack(t.getOffset(), "c1");
        assertEquals(AckStatus.ALREADY_ACKED, duplicateAck.status(),
                "重复确认必须是无害的确定结果，不重复消费");
        assertEquals(1L, queue.committedOffset());
    }

    @Test
    void expiredLease_redeliversOnceAtATime_andStaleAckIsRejected() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 5, 30_000L, 1_000L, 60_000L);

        String key = "order-1003";
        queue.enqueue(key, "email");
        TaskRecord first = queue.poll("slow-consumer");
        assertEquals(1, first.getAttempts());

        // 租约未过期：其他消费者不能抢占；旧消费者自己也不能重复领取
        assertNull(queue.poll("fast-consumer"));

        clock.advance(30_001L);
        TaskRecord redelivered = queue.poll("fast-consumer");
        log.info("[test] key='{}' redelivered after lease expiry, attempt={}, owner='{}'",
                key, redelivered.getAttempts(), redelivered.getOwnerToken());
        assertNotNull(redelivered);
        assertSame(first, redelivered);
        assertEquals(2, redelivered.getAttempts(), "至少一次投递：租约过期后重新投递，投递次数累加");

        // 旧消费者晚到的 ack 必须被明确拒绝，不能把新消费者的工作标记掉
        AckOutcome stale = queue.ack(first.getOffset(), "slow-consumer");
        log.info("[test] key='{}' stale ack from old owner -> {}", key, stale.status());
        assertEquals(AckStatus.LEASE_EXPIRED, stale.status());
        assertEquals(TaskState.RUNNING, redelivered.getState());

        assertNull(queue.poll("third-consumer"), "重新投递后新租约期内仍然只能有一个占用者");
    }
}
