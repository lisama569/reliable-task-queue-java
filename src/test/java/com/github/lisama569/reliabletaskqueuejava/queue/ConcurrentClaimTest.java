package com.github.lisama569.reliabletaskqueuejava.queue;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 场景二：并发抢占与至少一次投递。
 * <ul>
 *   <li>多消费者并发领取同一批任务：每个任务恰好被一个消费者占用（无重复、无丢失）。</li>
 *   <li>租约到期未确认：任务被重新投递给其他消费者（重复投递），旧持有者的 ack 被拒绝。</li>
 * </ul>
 */
class ConcurrentClaimTest {

    private static final Logger log = LoggerFactory.getLogger(ConcurrentClaimTest.class);

    private final AtomicLong now = new AtomicLong(1_000_000);

    private TaskQueue newQueue(Path dir, int maxAttempts, Duration lease) {
        QueueConfig config = new QueueConfig(
                lease, maxAttempts, Duration.ofMillis(50), Duration.ofSeconds(5),
                dir.resolve("snap.json").toString());
        return new TaskQueue(config, new JsonSnapshotStore(dir.resolve("snap.json")), now::get);
    }

    @Test
    void concurrentClaimersEachTaskOwnedByExactlyOneConsumer(@TempDir Path dir) throws Exception {
        int taskCount = 50;
        int consumerCount = 8;
        TaskQueue queue = newQueue(dir, 5, Duration.ofSeconds(30));
        for (int i = 0; i < taskCount; i++) {
            queue.submit("job-" + i, "p" + i);
        }

        ExecutorService pool = Executors.newFixedThreadPool(consumerCount);
        CountDownLatch start = new CountDownLatch(1);
        ConcurrentHashMap<Long, String> ownerById = new ConcurrentHashMap<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        try {
            java.util.List<Future<?>> futures = new java.util.ArrayList<>();
            for (int c = 0; c < consumerCount; c++) {
                String consumer = "consumer-" + c;
                futures.add(pool.submit(() -> {
                    try {
                        start.await();
                        while (true) {
                            TaskRecord t = queue.claim(consumer);
                            if (t == null) {
                                return;
                            }
                            String previous = ownerById.putIfAbsent(t.getId(), consumer);
                            if (previous != null) {
                                throw new AssertionError(
                                        "任务 " + t.getId() + " 被并发重复领取: " + previous + " 与 " + consumer);
                            }
                            queue.ack(t.getId(), t.getDeliveryTag(), consumer);
                        }
                    } catch (Throwable e) {
                        failure.compareAndSet(e, e);
                    }
                }));
            }
            start.countDown();
            for (Future<?> f : futures) {
                f.get();
            }
        } finally {
            pool.shutdownNow();
        }

        log.info("[断言] {} 个任务被 {} 个并发消费者全部领取且每个仅被一个占用，占用表大小={}",
                taskCount, consumerCount, ownerById.size());
        assertNull(failure.get(), "并发抢占过程中不应出现重复领取");
        assertEquals(taskCount, ownerById.size(), "每个任务必须恰好被一个消费者领取");
        assertEquals(taskCount, queue.listTasks().stream()
                .filter(t -> t.getStatus() == TaskStatus.COMPLETED).count());
        log.info("[断言] key=job-* 全部 {} 个任务最终状态=COMPLETED，无重复执行无丢失", taskCount);
    }

    @Test
    void expiredLeaseTriggersRedeliveryAndStaleAckIsRejected(@TempDir Path dir) {
        TaskQueue queue = newQueue(dir, 5, Duration.ofSeconds(30));
        queue.submit("lease-1", "payload");

        TaskRecord first = queue.claim("c1");
        assertNotNull(first);
        assertEquals("c1", first.getLeasedBy());
        final long firstTag = first.getDeliveryTag();
        log.info("[断言] key=lease-1 首次投递 deliveryTag={} 占用者=c1，租约到期={}",
                firstTag, first.getLeaseExpiresAt());

        // 租约内 c2 抢不到
        assertNull(queue.claim("c2"), "租约未过期时其他消费者不得抢占");

        // 时间推进超过租约：c1 崩溃未确认，c2 重新领取（至少一次 → 重复投递）
        now.addAndGet(Duration.ofSeconds(31).toMillis());
        TaskRecord second = queue.claim("c2");
        assertNotNull(second);
        assertEquals("c2", second.getLeasedBy());
        assertEquals(firstTag + 1, second.getDeliveryTag(),
                "重复投递必须使用新的 deliveryTag");
        log.info("[断言] key=lease-1 租约过期后重复投递给 c2，deliveryTag: {} -> {}",
                firstTag, second.getDeliveryTag());

        // 旧持有者 c1 迟到的 ack 必须被明确拒绝，而不是让旧消费者影响新占用
        QueueException.LeaseMismatch ex = assertThrows(QueueException.LeaseMismatch.class,
                () -> queue.ack(first.getId(), first.getDeliveryTag(), "c1"));
        assertEquals("LEASE_MISMATCH", ex.getCode());
        log.info("[断言] key=lease-1 旧持有者 c1 的迟到 ack 被拒绝 code={}（最终以 c2 的确认为准）",
                ex.getCode());

        queue.ack(second.getId(), second.getDeliveryTag(), "c2");
        assertEquals(TaskStatus.COMPLETED, queue.getTask(first.getId()).getStatus());
        now.addAndGet(Duration.ofSeconds(100).toMillis());
        assertNull(queue.claim("c3"), "已确认任务不再投递");
        log.info("[断言] key=lease-1 经 c2 确认后 status=COMPLETED，c3 claim=null");
    }
}
