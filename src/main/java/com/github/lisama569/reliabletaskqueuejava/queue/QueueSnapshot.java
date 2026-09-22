package com.github.lisama569.reliabletaskqueuejava.queue;

import java.util.ArrayList;
import java.util.List;

/**
 * 需要跨进程重启持久化的全部队列状态快照。
 */
public class QueueSnapshot {

    private long nextId = 1;
    private long committedOffset = 0;
    private List<TaskRecord> tasks = new ArrayList<>();

    public long getNextId() {
        return nextId;
    }

    public void setNextId(long nextId) {
        this.nextId = nextId;
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
