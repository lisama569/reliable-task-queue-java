package com.github.lisama569.reliabletaskqueuejava.queue;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：通过 HTTP 验证幂等提交、抢占/确认、租约拒绝、位点拒绝、死信与显式处置，
 * 以及可区分的错误码。退避基准在配置中设为 0，使重试立即可见。
 */
@SpringBootTest
@AutoConfigureMockMvc
class QueueApiIntegrationTest {

    private static Path snapshotFile;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) throws Exception {
        snapshotFile = Files.createTempFile("q-it-", ".json");
        Files.deleteIfExists(snapshotFile);
        registry.add("taskqueue.snapshot-file", () -> snapshotFile.toString());
        registry.add("taskqueue.lease-seconds", () -> 30);
        registry.add("taskqueue.max-attempts", () -> 2);
        registry.add("taskqueue.backoff-base-millis", () -> 0);
        registry.add("taskqueue.max-backoff-millis", () -> 0);
    }

    @Autowired
    private MockMvc mvc;

    @AfterAll
    static void cleanup() throws Exception {
        if (snapshotFile != null) {
            Files.deleteIfExists(snapshotFile);
        }
    }

    private static long tagOf(String json) {
        Matcher m = Pattern.compile("\"deliveryTag\":(\\d+)").matcher(json);
        if (!m.find()) {
            throw new IllegalStateException("响应中没有 deliveryTag: " + json);
        }
        return Long.parseLong(m.group(1));
    }

    @Test
    void fullLifecycleOverHttp() throws Exception {
        // 1) 首次入队 201 ACCEPTED；重复提交 200 DUPLICATE_IN_FLIGHT
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"http-1","payload":"p"}
                                """))
                .andExpect(status().isCreated())
                .andExpect(jsonPath("$.outcome").value("ACCEPTED"))
                .andExpect(jsonPath("$.task.id").value(1));

        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"http-1","payload":"p2"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("DUPLICATE_IN_FLIGHT"))
                .andExpect(jsonPath("$.task.id").value(1));

        // 2) c1 领取
        String claimed = mvc.perform(post("/api/tasks/claim").param("consumerId", "c1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.task.leasedBy").value("c1"))
                .andReturn().getResponse().getContentAsString();
        long tag1 = tagOf(claimed);

        // 3) 任务仍被 c1 占用时，其他消费者用不匹配的 deliveryTag 确认 -> 409 LEASE_MISMATCH
        mvc.perform(post("/api/tasks/1/ack").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"stale","deliveryTag":999}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("LEASE_MISMATCH"));

        // 4) 第 1 次失败 -> PENDING 重试（退避=0，立即可再领）
        mvc.perform(post("/api/tasks/1/nack").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"c1","deliveryTag":%d,"error":"ERR_HTTP_500"}
                                """.formatted(tag1)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("PENDING"))
                .andExpect(jsonPath("$.lastError").value("ERR_HTTP_500"));

        // 5) c2 重投，第 2 次失败（attempts 达 maxAttempts=2）-> DEAD，原因可区分
        String claimed2 = mvc.perform(post("/api/tasks/claim").param("consumerId", "c2"))
                .andExpect(jsonPath("$.task.id").value(1))
                .andExpect(jsonPath("$.task.leasedBy").value("c2"))
                .andReturn().getResponse().getContentAsString();
        long tag2 = tagOf(claimed2);
        mvc.perform(post("/api/tasks/1/nack").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"c2","deliveryTag":%d,"error":"ERR_PERMANENT"}
                                """.formatted(tag2)))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DEAD"))
                .andExpect(jsonPath("$.lastError").value("ERR_PERMANENT"));

        // 6) 对死信任务 ack -> 409 ILLEGAL_TASK_STATE（状态终态，拒绝任何确认）
        mvc.perform(post("/api/tasks/1/ack").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"c2","deliveryTag":%d}
                                """.formatted(tag2)))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("ILLEGAL_TASK_STATE"));

        // 7) 位点回退（到当前位点 0 之前）-> 409 OFFSET_REWIND_REJECTED
        mvc.perform(post("/api/offset/commit").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"consumerId":"c1","offset":-1}
                                """))
                .andExpect(status().isConflict())
                .andExpect(jsonPath("$.code").value("OFFSET_REWIND_REJECTED"));

        // 8) 同键再次提交死信任务 -> ALREADY_DEAD，确定不执行
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"http-1","payload":"p3"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ALREADY_DEAD"));

        // 9) 显式处置死信 -> COMPLETED，保留原因与决策，位点可推进到 1
        mvc.perform(post("/api/tasks/1/resolve").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"operator":"alice","resolution":"人工补偿，视为完成"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("COMPLETED"))
                .andExpect(jsonPath("$.lastError").value("ERR_PERMANENT"))
                .andExpect(jsonPath("$.resolution").value("[RESOLVED by alice] 人工补偿，视为完成"));
        mvc.perform(post("/api/offset/advance"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedOffset").value(1));

        // 10) 处置后同键再提交 -> ALREADY_COMPLETED，绝不重复执行
        mvc.perform(post("/api/tasks").contentType(MediaType.APPLICATION_JSON)
                        .content("""
                                {"idempotencyKey":"http-1","payload":"p4"}
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.outcome").value("ALREADY_COMPLETED"));

        mvc.perform(get("/api/offset")).andExpect(jsonPath("$.committedOffset").value(1));
    }
}
