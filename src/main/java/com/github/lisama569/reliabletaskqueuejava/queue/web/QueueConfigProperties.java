package com.github.lisama569.reliabletaskqueuejava.queue.web;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * 队列可配置属性（前缀 taskqueue）。
 */
@ConfigurationProperties(prefix = "taskqueue")
public class QueueConfigProperties {

    private long leaseSeconds = 30;
    private int maxAttempts = 3;
    private long backoffBaseMillis = 200;
    private long maxBackoffMillis = 10_000;
    /** 快照文件路径；默认 target 下的本地文件，仅用于本地演示。 */
    private String snapshotFile = "target/taskqueue-snapshot.json";

    public long getLeaseSeconds() {
        return leaseSeconds;
    }

    public void setLeaseSeconds(long leaseSeconds) {
        this.leaseSeconds = leaseSeconds;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public long getBackoffBaseMillis() {
        return backoffBaseMillis;
    }

    public void setBackoffBaseMillis(long backoffBaseMillis) {
        this.backoffBaseMillis = backoffBaseMillis;
    }

    public long getMaxBackoffMillis() {
        return maxBackoffMillis;
    }

    public void setMaxBackoffMillis(long maxBackoffMillis) {
        this.maxBackoffMillis = maxBackoffMillis;
    }

    public String getSnapshotFile() {
        return snapshotFile;
    }

    public void setSnapshotFile(String snapshotFile) {
        this.snapshotFile = snapshotFile;
    }
}
