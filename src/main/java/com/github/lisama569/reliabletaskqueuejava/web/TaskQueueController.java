package com.github.lisama569.reliabletaskqueuejava.web;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueResult;
import com.github.lisama569.reliabletaskqueuejava.queue.OffsetRollbackException;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskQueue;
import com.github.lisama569.reliabletaskqueuejava.queue.TaskRecord;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * 任务队列 HTTP 演示接口。
 */
@RestController
@RequestMapping("/api/tasks")
public class TaskQueueController {

    private final TaskQueue queue;

    public TaskQueueController(TaskQueue queue) {
        this.queue = queue;
    }

    /** 幂等提交：body 为 {"key":"...", "payload":"..."}。 */
    @PostMapping
    public EnqueueResult enqueue(@RequestBody Map<String, String> body) {
        String key = body.get("key");
        String payload = body.get("payload");
        return queue.enqueue(key, payload);
    }

    /** 拉取并占用一个任务（租约）。无任务时返回 204。 */
    @PostMapping("/poll")
    public ResponseEntity<TaskRecord> poll(@RequestParam String owner) {
        TaskRecord t = queue.poll(owner);
        return t == null ? ResponseEntity.noContent().build() : ResponseEntity.ok(t);
    }

    /** 成功确认。 */
    @PostMapping("/{offset}/ack")
    public AckOutcome ack(@PathVariable long offset, @RequestParam String owner) {
        return queue.ack(offset, owner);
    }

    /** 失败上报（重试/死信由队列按策略决定）。 */
    @PostMapping("/{offset}/nack")
    public AckOutcome nack(@PathVariable long offset,
                           @RequestParam String owner,
                           @RequestParam(required = false) String reason) {
        return queue.nack(offset, owner, reason);
    }

    @GetMapping("/{offset}")
    public TaskRecord get(@PathVariable long offset) {
        return queue.get(offset);
    }

    @GetMapping
    public List<TaskRecord> list() {
        return queue.listAll();
    }

    @GetMapping("/dead-letters")
    public List<TaskRecord> deadLetters() {
        return queue.listDeadLetters();
    }

    @GetMapping("/committed-offset")
    public Map<String, Long> committedOffset() {
        return Map.of("committedOffset", queue.committedOffset());
    }

    /**
     * 显式推进位点。回退或跨越未确认任务会被队列拒绝（409），
     * 防止重复消费已确认的任务。
     */
    @PostMapping("/committed-offset/{target}")
    public Map<String, Long> seek(@PathVariable long target) {
        queue.seek(target);
        return Map.of("committedOffset", queue.committedOffset());
    }

    @ExceptionHandler(OffsetRollbackException.class)
    public ResponseEntity<String> handleRollback(OffsetRollbackException e) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(e.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<String> handleBadRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(e.getMessage());
    }
}
