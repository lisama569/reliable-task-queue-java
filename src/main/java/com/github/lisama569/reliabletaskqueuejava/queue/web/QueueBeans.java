package com.github.lisama569.reliabletaskqueuejava.queue.web;

import com.github.lisama569.reliabletaskqueuejava.queue.JsonSnapshotStore;
import com.github.lisama569.reliabletaskqueuejava.queue.QueueConfig;
import com.github.lisama569.reliabletaskqueuejava.queue.SnapshotStore;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskQueue;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.nio.file.Path;
import java.time.Duration;

/**
 * 队列组件的 Spring 装配。
 */
@Configuration
@EnableConfigurationProperties(QueueConfigProperties.class)
public class QueueBeans {

    @Bean
    public SnapshotStore snapshotStore(QueueConfigProperties props) {
        String file = props.getSnapshotFile();
        return new JsonSnapshotStore(file == null || file.isBlank() ? null : Path.of(file));
    }

    @Bean
    public TaskQueue taskQueue(QueueConfigProperties props, SnapshotStore snapshotStore) {
        QueueConfig config = new QueueConfig(
                Duration.ofSeconds(props.getLeaseSeconds()),
                props.getMaxAttempts(),
                Duration.ofMillis(props.getBackoffBaseMillis()),
                Duration.ofMillis(props.getMaxBackoffMillis()),
                props.getSnapshotFile());
        return new TaskQueue(config, snapshotStore);
    }
}
