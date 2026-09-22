package com.github.lisama569.reliabletaskqueuejava.queue.web;

/**
 * 领取响应。无可消费任务时 task 为 null。
 */
public record ClaimResponse(String consumerId, TaskView task) {
}
