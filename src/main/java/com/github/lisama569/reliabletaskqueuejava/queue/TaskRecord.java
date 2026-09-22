package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 任务记录（可变状态机实体）。任务一经创建，序号（offset）与幂等键即不可变。
 */
public class TaskRecord {

    private long offset;
    private String idempotencyKey;
    private String payload;
    private TaskState state;
    private int attempts;
    private int maxAttempts;
    private String ownerToken;
    private long leaseExpireAtMillis;
    private long enqueueAtMillis;
    private long nextVisibleAtMillis;
    private long completedAtMillis;
    private String failureReason;

    public TaskRecord() {
    }

    public long getOffset() {
        return offset;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public String getIdempotencyKey() {
        return idempotencyKey;
    }

    public void setIdempotencyKey(String idempotencyKey) {
        this.idempotencyKey = idempotencyKey;
    }

    public String getPayload() {
        return payload;
    }

    public void setPayload(String payload) {
        this.payload = payload;
    }

    public TaskState getState() {
        return state;
    }

    public void setState(TaskState state) {
        this.state = state;
    }

    public int getAttempts() {
        return attempts;
    }

    public void setAttempts(int attempts) {
        this.attempts = attempts;
    }

    public int getMaxAttempts() {
        return maxAttempts;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

    public String getOwnerToken() {
        return ownerToken;
    }

    public void setOwnerToken(String ownerToken) {
        this.ownerToken = ownerToken;
    }

    public long getLeaseExpireAtMillis() {
        return leaseExpireAtMillis;
    }

    public void setLeaseExpireAtMillis(long leaseExpireAtMillis) {
        this.leaseExpireAtMillis = leaseExpireAtMillis;
    }

    public long getEnqueueAtMillis() {
        return enqueueAtMillis;
    }

    public void setEnqueueAtMillis(long enqueueAtMillis) {
        this.enqueueAtMillis = enqueueAtMillis;
    }

    public long getNextVisibleAtMillis() {
        return nextVisibleAtMillis;
    }

    public void setNextVisibleAtMillis(long nextVisibleAtMillis) {
        this.nextVisibleAtMillis = nextVisibleAtMillis;
    }

    public long getCompletedAtMillis() {
        return completedAtMillis;
    }

    public void setCompletedAtMillis(long completedAtMillis) {
        this.completedAtMillis = completedAtMillis;
    }

    public String getFailureReason() {
        return failureReason;
    }

    public void setFailureReason(String failureReason) {
        this.failureReason = failureReason;
    }

    @Override
    public String toString() {
        return "TaskRecord{offset=" + offset
                + ", key='" + idempotencyKey + '\''
                + ", state=" + state
                + ", attempts=" + attempts + '/' + maxAttempts
                + ", owner='" + ownerToken + '\''
                + ", failureReason='" + failureReason + '\''
                + '}';
    }
}
