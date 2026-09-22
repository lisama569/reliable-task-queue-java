package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 快照持久化抽象（用本地 JSON 文件模拟消息代理的持久化）。
 */
public interface SnapshotStore {

    /** 启动时读取快照；不存在时返回 null。 */
    QueueSnapshot load();

    /** 原子地覆盖持久化最新快照。 */
    void save(QueueSnapshot snapshot);
}
