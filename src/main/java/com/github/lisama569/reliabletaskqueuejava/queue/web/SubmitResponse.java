package com.github.lisama569.reliabletaskqueuejava.queue.web;

/**
 * 入队响应：outcome 给出确定且可解释的结果，task 给出对应任务视图。
 */
public record SubmitResponse(String outcome, String explanation, TaskView task) {
}
