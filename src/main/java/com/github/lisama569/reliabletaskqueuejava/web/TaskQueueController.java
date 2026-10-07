package com.github.lisama569.reliabletaskqueuejava.web;

import com.github.lisama569.reliabletaskqueuejava.queue.AckOutcome;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueResult;
import com.github.lisama569.reliabletaskqueuejava.queue.EnqueueStatus;
import com.github.lisama569.reliabletaskqueuejava.queue.OffsetRollbackException;
import com.github.lisama569.reliabletaskqueuejava.queue.RenewOutcome;
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

    /**
     * 幂等提交：body 为 {"key":"...", "payload":"..."}。
     * 达到容量上限时返回 429 与 {@link EnqueueStatus#QUEUE_FULL}（背压，任务不会被静默丢弃）。
     */
    @PostMapping
    public ResponseEntity<EnqueueResult> enqueue(@RequestBody Map<String, String> body) {
        String key = body.get("key");
        String payload = body.get("payload");
        EnqueueResult result = queue.enqueue(key, payload);
        if (result.status() == EnqueueStatus.QUEUE_FULL) {
            return ResponseEntity.status(HttpStatus.TOO_MANY_REQUESTS).body(result);
        }
        return ResponseEntity.ok(result);
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

    /** 租约续期（长任务心跳）：仅当前有效占用者可续，失败后原占用者也无法再确认。 */
    @PostMapping("/{offset}/renew")
    public RenewOutcome renew(@PathVariable long offset, @RequestParam String owner) {
        return queue.renew(offset, owner);
    }

    /** 主动放弃占用：任务立即重新可领取；放弃后原占用者的确认会被拒绝。 */
    @PostMapping("/{offset}/abandon")
    public AckOutcome abandon(@PathVariable long offset,
                              @RequestParam String owner,
                              @RequestParam(required = false) String reason) {
        return queue.abandon(offset, owner, reason);
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

    /** 容量与当前占用（capacity&lt;=0 表示不限制）。 */
    @GetMapping("/capacity")
    public Map<String, Integer> capacity() {
        return Map.of("capacity", queue.capacity(), "currentSize", queue.currentSize());
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
