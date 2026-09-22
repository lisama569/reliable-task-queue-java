package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.AckStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueResult;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.RetryPolicy;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskState;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 覆盖：指数退避的可解释性、重试次数耗尽后进入死信、
 * 死信保留可区分的失败原因、死信不会被再次投递、位点被死信挡住、
 * 死信后重新提交给出 ALREADY_DEAD 确定结果（不执行也不报冲突）。
 */
class RetryAndDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryAndDeadLetterTest.class);

    @TempDir
    Path tempDir;

    @Test
    void retryBackoffIsDeterministicExponentialSchedule() {
        RetryPolicy policy = new RetryPolicy(1_000L, 10_000L);
        assertEquals(1_000L, policy.backoffMillis(1));
        assertEquals(2_000L, policy.backoffMillis(2));
        assertEquals(4_000L, policy.backoffMillis(3));
        assertEquals(8_000L, policy.backoffMillis(4));
        assertEquals(10_000L, policy.backoffMillis(5), "退避必须有明确上限");
        log.info("[test] backoff schedule (base=1000,max=10000): {} {} {} {} {}",
                policy.backoffMillis(1), policy.backoffMillis(2), policy.backoffMillis(3),
                policy.backoffMillis(4), policy.backoffMillis(5));
    }

    @Test
    void failingTaskRetriesWithBackoffThenDeadLettersWithDistinctReason() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap.json"), clock, 3, 30_000L, 1_000L, 60_000L);

        String key = "job-dead-1";
        queue.enqueue(key, "flaky");

        // attempt 1 失败 -> 退避 1s
        TaskRecord a1 = queue.poll("c1");
        assertEquals(1, a1.getAttempts());
        clock.advance(500L);
        assertNull(queue.poll("c2"), "退避窗口内任务不可见，不得提前投递");
        AckOutcome n1 = queue.nack(a1.getOffset(), "c1", "NullPointerException at stage A");
        assertEquals(AckStatus.APPLIED, n1.status());
        assertEquals(TaskState.PENDING, a1.getState());
        assertEquals(1_000_000L + 500L + 1_000L, a1.getNextVisibleAtMillis());
        log.info("[test] key='{}' after attempt1 state={} nextVisibleAt={}",
                key, a1.getState(), a1.getNextVisibleAtMillis());

        assertNull(queue.poll("c2"), "仍在退避窗口内");
        clock.advance(1_001L);

        // attempt 2 失败 -> 退避 2s
        TaskRecord a2 = queue.poll("c2");
        assertNotNull(a2);
        assertEquals(2, a2.getAttempts());
        clock.advance(10L);
        queue.nack(a2.getOffset(), "c2", "Timeout calling downstream B");
        assertEquals(1_000_000L + 500L + 1_001L + 10L + 2_000L, a2.getNextVisibleAtMillis());
        log.info("[test] key='{}' after attempt2 state={} nextVisibleAt={}",
                key, a2.getState(), a2.getNextVisibleAtMillis());

        clock.advance(2_001L);

        // attempt 3（最后一次）失败 -> DEAD，原因保留为最后一次失败原因
        TaskRecord a3 = queue.poll("c3");
        assertEquals(3, a3.getAttempts());
        AckOutcome n3 = queue.nack(a3.getOffset(), "c3", "HTTP 500 from payment gateway");
        assertEquals(AckStatus.APPLIED, n3.status());
        assertEquals(TaskState.DEAD, a3.getState());
        assertEquals("HTTP 500 from payment gateway", a3.getFailureReason(),
                "死信必须保留可区分的失败原因，不得静默丢弃");
        log.info("[test] key='{}' transition -> DEAD reason='{}'", key, a3.getFailureReason());

        // 死信不再被投递；位点为 0（死信挡住连续成功前缀）
        clock.advance(100_000L);
        assertNull(queue.poll("c4"), "死信任务永远不会被再次投递");
        assertEquals(0L, queue.committedOffset(), "未确认的死信不能推进位点");

        List<TaskRecord> dead = queue.listDeadLetters();
        assertEquals(1, dead.size());
        assertEquals(3, dead.get(0).getAttempts());
        assertEquals(key, dead.get(0).getIdempotencyKey());

        // 死信后再次提交：确定结果 ALREADY_DEAD，不执行也不报冲突
        EnqueueResult resubmit = queue.enqueue(key, "flaky-again");
        assertEquals(EnqueueStatus.ALREADY_DEAD, resubmit.status());
        assertEquals(a3, resubmit.task());
        assertEquals(1, queue.listAll().size(), "不能产生重复任务");

        // 对死信做 ack 得到可区分结果
        assertEquals(AckStatus.DEAD, queue.ack(a3.getOffset(), "c3").status());
        assertTrue(queue.get(a3.getOffset()).getCompletedAtMillis() > 0);
    }

    @Test
    void deadLetterBlocksOffsetWhileLaterTasksCanStillComplete() {
        TestClock clock = new TestClock(1_000_000L);
        InMemoryReliableTaskQueue queue = QueueTestSupport.newQueue(
                tempDir.resolve("snap2.json"), clock, 1, 30_000L, 10L, 100L);

        queue.enqueue("k0", "p0");
        queue.enqueue("k1", "p1");
        queue.enqueue("k2", "p2");

        // k0 被 c 占用（飞行中）；飞行中任务不阻塞后续任务的投递，但会阻塞位点
        TaskRecord t0 = queue.poll("c");
        assertEquals(0, t0.getOffset());
        TaskRecord t1 = queue.poll("c2");
        assertEquals(1, t1.getOffset(), "飞行中的 k0 不阻塞 k1 的投递");
        AckOutcome ack1 = queue.ack(1, "c2");
        assertEquals(AckStatus.APPLIED, ack1.status());
        assertEquals(0L, queue.committedOffset(), "k0 未确认，位点不能越过它");

        TaskRecord t2 = queue.poll("c2");
        assertEquals(2, t2.getOffset());
        queue.ack(2, "c2");
        assertEquals(0L, queue.committedOffset(), "即使 k1/k2 已确认，位点仍被未确认的 k0 挡住");

        // k0 仅允许 1 次投递，失败即耗尽 -> DEAD，位点永远停在 0
        queue.nack(t0.getOffset(), "c", "permanent failure X");
        assertEquals(TaskState.DEAD, t0.getState());
        assertEquals(TaskState.SUCCEEDED, queue.get(1).getState());
        assertEquals(TaskState.SUCCEEDED, queue.get(2).getState());

        log.info("[test] states: t0={}, t1={}, t2={}, committedOffset={}",
                queue.get(0).getState(), queue.get(1).getState(), queue.get(2).getState(),
                queue.committedOffset());
        assertEquals(0L, queue.committedOffset(),
                "死信 k0 挡住位点：后续任务的确认不能让位点越过未确认任务");
        assertEquals(1, queue.listDeadLetters().size());
    }
}
