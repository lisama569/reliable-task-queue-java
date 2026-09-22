package com.github.lisama569.reliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * 场景三：消费失败的指数退避重试，以及重试耗尽进入死信。
 * 死信任务不再被投递，保留可区分的最后失败原因，且重复提交给出确定结果（不静默丢弃）。
 */
class RetryDeadLetterTest {

    private static final Logger log = LoggerFactory.getLogger(RetryDeadLetterTest.class);

    private final AtomicLong now = new AtomicLong(1_000_000);

    private TaskQueue newQueue(Path dir) {
        QueueConfig config = new QueueConfig(
                Duration.ofSeconds(30), 3, Duration.ofMillis(100), Duration.ofSeconds(5),
                dir.resolve("snap.json").toString());
        return new TaskQueue(config, new JsonSnapshotStore(dir.resolve("snap.json")), now::get);
    }

    @Test
    void backsOffThenDeadLettersAfterMaxAttemptsWithDistinctReason(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir);
        queue.submit("flaky-1", "payload");

        // 第 1 次投递失败 -> 退避重试
        TaskRecord d1 = queue.claim("c1");
        assertEquals(1, d1.getAttempts());
        long afterFirstNack = now.get();
        queue.nack(d1.getId(), d1.getDeliveryTag(), "c1", "ERR_DOWNSTREAM_TIMEOUT");
        TaskRecord after1 = queue.getTask(1);
        assertEquals(TaskStatus.PENDING, after1.getStatus());
        assertEquals("ERR_DOWNSTREAM_TIMEOUT", after1.getLastError());
        long expectedBackoff = 100; // base * 2^(attempt-1) = 100 * 1
        assertEquals(afterFirstNack + expectedBackoff, after1.getNotVisibleUntil());
        log.info("[断言] key=flaky-1 第1次失败 status=PENDING attempts=1 退避{}ms 原因={}",
                expectedBackoff, after1.getLastError());

        // 退避窗口内不可领取
        now.addAndGet(expectedBackoff - 1);
        assertNull(queue.claim("c2"), "退避窗口内任务不可见");

        // 第 2 次投递失败 -> 退避翻倍
        now.addAndGet(1);
        TaskRecord d2 = queue.claim("c2");
        assertEquals(2, d2.getAttempts());
        long t2 = now.get();
        queue.nack(d2.getId(), d2.getDeliveryTag(), "c2", "ERR_HTTP_500");
        TaskRecord after2 = queue.getTask(1);
        assertEquals(TaskStatus.PENDING, after2.getStatus());
        assertEquals(t2 + 200, after2.getNotVisibleUntil(), "第二次失败退避应为 200ms（指数翻倍）");
        log.info("[断言] key=flaky-1 第2次失败 status=PENDING attempts=2 退避200ms 原因={}",
                after2.getLastError());

        // 第 3 次投递（attempts 达到 maxAttempts=3）失败 -> 死信
        now.addAndGet(200);
        TaskRecord d3 = queue.claim("c3");
        assertEquals(3, d3.getAttempts());
        queue.nack(d3.getId(), d3.getDeliveryTag(), "c3", "ERR_PERMANENT_BAD_PAYLOAD");
        TaskRecord dead = queue.getTask(1);
        assertEquals(TaskStatus.DEAD, dead.getStatus());
        assertEquals(3, dead.getAttempts());
        assertEquals("ERR_PERMANENT_BAD_PAYLOAD", dead.getLastError(),
                "死信必须保留可区分的失败原因，不得静默丢弃");
        log.info("[断言] key=flaky-1 重试耗尽 status=DEAD attempts=3/3 最终原因={}（可区分、可追溯）",
                dead.getLastError());

        // 死信永不再投递
        now.addAndGet(Duration.ofHours(1).toMillis());
        assertNull(queue.claim("c4"), "死信任务不得再被投递");
        log.info("[断言] key=flaky-1 时间大幅推进后 claim=null：死信不重复投递");

        // 死信任务用同键再次提交：确定结果 ALREADY_DEAD，不重新执行也不报冲突
        SubmitResult resubmit = queue.submit("flaky-1", "payload-new");
        assertEquals(SubmitOutcome.ALREADY_DEAD, resubmit.outcome());
        assertEquals(1, queue.listTasks().size());
        assertEquals(TaskStatus.DEAD, resubmit.task().getStatus());
        log.info("[断言] key=flaky-1 死信后重复提交 outcome=ALREADY_DEAD id=1 status=DEAD");
    }

    @Test
    void retryThenSuccessDoesNotReachDeadLetter(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir);
        queue.submit("flaky-2", "payload");

        TaskRecord d1 = queue.claim("c1");
        queue.nack(d1.getId(), d1.getDeliveryTag(), "c1", "ERR_TRANSIENT");
        now.addAndGet(100);
        TaskRecord d2 = queue.claim("c2");
        assertNotNull(d2);
        queue.ack(d2.getId(), d2.getDeliveryTag(), "c2");

        TaskRecord done = queue.getTask(1);
        assertEquals(TaskStatus.COMPLETED, done.getStatus());
        assertEquals(2, done.getAttempts());
        log.info("[断言] key=flaky-2 第2次尝试确认完成 status=COMPLETED attempts=2，未进入死信");
    }
}
