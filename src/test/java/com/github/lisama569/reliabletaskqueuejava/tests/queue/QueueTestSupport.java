package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.JsonFileTaskQueueStore;
import com.github.lisama569.reliabletaskqueuejava.queue.RetryPolicy;

import java.nio.file.Path;

/** 构造使用临时快照文件与可控时钟的队列，便于确定性测试。 */
final class QueueTestSupport {

    private QueueTestSupport() {
    }

    static InMemoryReliableTaskQueue newQueue(Path snapshot,
                                              TestClock clock,
                                              int maxAttempts,
                                              long leaseMillis,
                                              long retryBaseMillis,
                                              long retryMaxMillis) {
        JsonFileTaskQueueStore store =
                new JsonFileTaskQueueStore(snapshot, new ObjectMapper());
        RetryPolicy retryPolicy = new RetryPolicy(retryBaseMillis, retryMaxMillis);
        InMemoryReliableTaskQueue queue =
                new InMemoryReliableTaskQueue(maxAttempts, leaseMillis, retryPolicy, store, clock);
        queue.recover();
        return queue;
    }
}
