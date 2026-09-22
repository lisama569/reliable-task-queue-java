package com.github.lisama569.reliabletaskqueuejava.queue;

import java.util.List;

/**
 * 可持久化的队列快照：全部任务 + 已提交位点。
 */
public class QueueSnapshot {

    private long committedOffset;
    private List<TaskRecord> tasks;

    public QueueSnapshot() {
    }

    public long getCommittedOffset() {
        return committedOffset;
    }

    public void setCommittedOffset(long committedOffset) {
        this.committedOffset = committedOffset;
    }

    public List<TaskRecord> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskRecord> tasks) {
        this.tasks = tasks;
    }
}
