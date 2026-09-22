package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 入队/幂等提交结果：包含确定状态与对应的任务记录。
 */
public record EnqueueResult(EnqueueStatus status, TaskRecord task) {
}
