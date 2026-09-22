package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 提交结果：包含确定的 {@link SubmitOutcome} 以及对应的任务（可能是新建或已有的）。
 */
public record SubmitResult(SubmitOutcome outcome, TaskRecord task) {
}
