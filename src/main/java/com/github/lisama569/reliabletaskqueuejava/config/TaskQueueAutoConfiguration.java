package com.github.lisama569.reliabletaskqueuejava.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.lisama569.reliabletaskqueuejava.queue.InMemoryReliableTaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.JsonFileTaskQueueStore;
import com.github.lisama569.reliabletaskqueuejava.queue.RetryPolicy;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskQueueStore;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Clock;

/**
 * 组装可靠任务队列相关 Bean。
 */
@Configuration
@EnableConfigurationProperties(QueueProperties.class)
public class TaskQueueAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    public Clock taskQueueClock() {
        return Clock.systemUTC();
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskQueueStore taskQueueStore(QueueProperties properties, ObjectMapper objectMapper) {
        return new JsonFileTaskQueueStore(Path.of(properties.getStorePath()), objectMapper);
    }

    @Bean
    @ConditionalOnMissingBean
    public TaskQueue taskQueue(QueueProperties properties, TaskQueueStore store, Clock taskQueueClock) {
        RetryPolicy retryPolicy = new RetryPolicy(properties.getRetryBaseMillis(),
                properties.getRetryMaxMillis());
        InMemoryReliableTaskQueue queue = new InMemoryReliableTaskQueue(
                properties.getMaxAttempts(),
                properties.getLeaseMillis(),
                properties.getMaxCapacity(),
                retryPolicy,
                store,
                taskQueueClock);
        queue.recover();
        return queue;
    }
}
