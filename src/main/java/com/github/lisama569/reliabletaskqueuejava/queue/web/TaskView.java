package com.github.lisama569.reliabletaskqueuejava.queue.web;

import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;

/**
 * 任务的对外只读视图，明确暴露状态流转所需字段与可区分的失败原因。
 */
public record TaskView(
        long id,
        String idempotencyKey,
        String payload,
        String status,
        int attempts,
        long deliveryTag,
        String leasedBy,
        long leaseExpiresAt,
        long notVisibleUntil,
        String lastError,
        String resolution,
        long completedAt
) {

    public static TaskView of(TaskRecord t) {
        return new TaskView(
                t.getId(),
                t.getIdempotencyKey(),
                t.getPayload(),
                t.getStatus().name(),
                t.getAttempts(),
                t.getDeliveryTag(),
                t.getLeasedBy(),
                t.getLeaseExpiresAt(),
                t.getNotVisibleUntil(),
                t.getLastError(),
                t.getResolution(),
                t.getCompletedAt());
    }
}
