package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 确认（ack/nack）操作的确定结果。
 */
public enum AckStatus {

    /** 操作（成功确认或失败上报）已生效。成功确认时可能同时推进位点。 */
    APPLIED,

    /** 任务此前已经确认成功，重复确认是无害的空操作，不会重复消费。 */
    ALREADY_ACKED,

    /** 租约已失效（过期、被其他消费者持有或任务已不再运行），本次确认被拒绝。 */
    LEASE_EXPIRED,

    /** 任务已进入死信，无法再确认。 */
    DEAD,

    /** 任务尚未被占用（没有有效租约），不能确认。 */
    NOT_RUNNING
}
