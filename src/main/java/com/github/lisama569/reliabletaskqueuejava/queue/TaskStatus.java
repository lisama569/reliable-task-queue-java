package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 任务生命周期状态。
 *
 * <ul>
 *   <li>{@link #PENDING}：已入队，等待被消费者领取。</li>
 *   <li>{@link #RUNNING}：已被某个消费者领取并占用（持有租约）。</li>
 *   <li>{@link #COMPLETED}：已被持有有效租约的消费者显式确认（ack），终态。</li>
 *   <li>{@link #DEAD}：重试次数耗尽后进入死信，终态，保留可区分的失败原因。</li>
 * </ul>
 */
public enum TaskStatus {
    PENDING,
    RUNNING,
    COMPLETED,
    DEAD
}
