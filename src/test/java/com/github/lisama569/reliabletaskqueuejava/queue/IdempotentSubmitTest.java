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
import static org.junit.jupiter.api.Assertions.assertSame;

/**
 * 场景一：重复投递（幂等提交）。
 * 同一幂等键在确认完成前重复提交不得造成重复执行；
 * 已完成后再次提交必须给出确定且可解释的结果，不能既执行又报冲突。
 */
class IdempotentSubmitTest {

    private static final Logger log = LoggerFactory.getLogger(IdempotentSubmitTest.class);

    private final AtomicLong now = new AtomicLong(1_000_000);

    private TaskQueue newQueue(Path dir) {
        QueueConfig config = new QueueConfig(
                Duration.ofSeconds(30), 3, Duration.ofMillis(50), Duration.ofSeconds(5),
                dir.resolve("snap.json").toString());
        return new TaskQueue(config, new JsonSnapshotStore(dir.resolve("snap.json")), now::get);
    }

    @Test
    void duplicateSubmissionBeforeCompletionDoesNotExecuteTwice(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir);

        SubmitResult first = queue.submit("order-1", "pay");
        SubmitResult second = queue.submit("order-1", "pay-again");
        // 让任务进入 RUNNING 后再提交一次，验证占用期间同样去重
        TaskRecord running = queue.claim("c-run");
        SubmitResult third = queue.submit("order-1", "pay-again-while-running");
        // 复位：租约尚未过期，占用方无法被自己“取消”，这里用 nack 将任务退回 PENDING 以便后续断言
        queue.nack(running.getId(), running.getDeliveryTag(), "c-run", "用例复位");
        now.addAndGet(Duration.ofSeconds(6).toMillis());

        log.info("[断言] key=order-1 首次={} 第二次(PENDING)={} 第三次(RUNNING)={}，任务数={}",
                first.outcome(), second.outcome(), third.outcome(), queue.listTasks().size());
        assertEquals(SubmitOutcome.ACCEPTED, first.outcome());
        assertEquals(SubmitOutcome.DUPLICATE_IN_FLIGHT, second.outcome());
        assertEquals(SubmitOutcome.DUPLICATE_IN_FLIGHT, third.outcome());
        // 重复提交全部指向同一个任务，且只有一条任务
        assertSame(first.task(), second.task());
        assertEquals(1, queue.listTasks().size());

        // 队列里只能领出这一个任务；它被占用期间第二次领取必须为 null（并发抢占）
        TaskRecord claimed = queue.claim("c1");
        assertNotNull(claimed);
        assertEquals(1, claimed.getId());
        assertNull(queue.claim("c2"), "任务已被 c1 占用，c2 不得重复领取");
        log.info("[断言] key=order-1 被 c1 占用后 c2 领取结果=null，未发生重复执行");

        queue.ack(claimed.getId(), claimed.getDeliveryTag(), "c1");
    }

    @Test
    void resubmitAfterCompletedReturnsDeterministicResult(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir);

        SubmitResult first = queue.submit("order-2", "ship");
        TaskRecord t = queue.claim("c1");
        queue.ack(t.getId(), t.getDeliveryTag(), "c1");

        SubmitResult resubmit = queue.submit("order-2", "ship-again");
        log.info("[断言] key=order-2 完成后再次提交 outcome={}，返回既有任务 id={} status={}",
                resubmit.outcome(), resubmit.task().getId(), resubmit.task().getStatus());
        assertEquals(SubmitOutcome.ALREADY_COMPLETED, resubmit.outcome());
        assertEquals(first.task().getId(), resubmit.task().getId());
        assertEquals(TaskStatus.COMPLETED, resubmit.task().getStatus());
        assertEquals(1, queue.listTasks().size());

        // 已完成任务绝不能被再次投递
        assertNull(queue.claim("c2"), "已确认完成的任务不得被重新投递");
        log.info("[断言] key=order-2 完成后 claim=null：没有重复执行，也没有报冲突");
    }
}
