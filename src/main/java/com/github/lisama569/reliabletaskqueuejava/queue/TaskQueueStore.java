package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 队列持久化存储：加载启动快照，并在每次状态变更后原子写入。
 * 用文件快照模拟消息代理的持久化能力，不依赖任何外部服务。
 */
public interface TaskQueueStore {

    /** 启动时加载快照；文件不存在时返回 null（全新队列）。 */
    QueueSnapshot load();

    /** 原子写入快照。 */
    void save(QueueSnapshot snapshot);
}
