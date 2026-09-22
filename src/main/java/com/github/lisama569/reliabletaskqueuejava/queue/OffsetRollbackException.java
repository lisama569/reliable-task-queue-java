package com.github.lisama569.reliabletaskqueuejava.queue;

/**
 * 位点回退（或跨越未确认任务）被明确拒绝时抛出，防止重复消费已确认的任务。
 */
public class OffsetRollbackException extends RuntimeException {

    public OffsetRollbackException(String message) {
        super(message);
    }
}
