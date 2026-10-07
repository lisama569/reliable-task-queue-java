package com.github.lisama569.reliabletaskqueuejava.tests.queue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;

import java.nio.file.Path;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：通过 HTTP 演示接口验证幂等提交、拉取、确认、续期、主动放弃，
 * 以及位点回退/跨越未确认任务被明确拒绝（409 CONFLICT）。
 *
 * <p>快照放在 {@link TempDir}，保证每次运行从空队列开始，测试之间互不污染。
 */
@SpringBootTest
@AutoConfigureMockMvc
class TaskQueueWebIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.store-path", () -> tempDir.resolve("web-it-snapshot.json").toString());
    }

    @Autowired
    private MockMvc mockMvc;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void enqueuePollAckLifecycle_andInvalidSeekReturns409() throws Exception {
        String key = "web-key-" + System.nanoTime();
        // 1) 幂等提交
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "\",\"payload\":\"hello\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"))
                .andExpect(jsonPath("$.task.state").value("PENDING"));
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "\",\"payload\":\"hello-again\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DEDUPLICATED"));

        // 2) 拉取占用（租约）
        String pollBody = mockMvc.perform(post("/api/tasks/poll").param("owner", "w1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andExpect(jsonPath("$.ownerToken").value("w1"))
                .andReturn().getResponse().getContentAsString();
        long offset = objectMapper.readTree(pollBody).get("offset").asLong();

        // 3) 此时该任务未确认，位点必然 <= offset；尝试越过它推进位点 -> 409
        mockMvc.perform(get("/api/tasks/committed-offset"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.committedOffset").value(
                        org.hamcrest.Matchers.lessThanOrEqualTo((int) offset)));

        mockMvc.perform(post("/api/tasks/committed-offset/" + (offset + 1)))
                .andExpect(status().isConflict())
                .andExpect(result -> org.junit.jupiter.api.Assertions.assertTrue(
                        result.getResolvedException() instanceof
                                com.github.lisama569.reliabletaskqueuejava.queue.OffsetRollbackException));

        // 4) 正确确认
        mockMvc.perform(post("/api/tasks/{offset}/ack", offset).param("owner", "w1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPLIED"));

        // 读取确认后的位点；只要位点 > 0，回退到 0 必须 409，已确认任务绝不重复消费
        JsonNode afterAck = objectMapper.readTree(mockMvc.perform(get("/api/tasks/committed-offset"))
                .andExpect(status().isOk())
                .andReturn().getResponse().getContentAsString());
        long committedAfterAck = afterAck.get("committedOffset").asLong();
        if (committedAfterAck > 0) {
            mockMvc.perform(post("/api/tasks/committed-offset/0"))
                    .andExpect(status().isConflict())
                    .andExpect(result -> org.junit.jupiter.api.Assertions.assertTrue(
                            result.getResolvedException() instanceof
                                    com.github.lisama569.reliabletaskqueuejava.queue.OffsetRollbackException));
        }

        // 5) 死信列表可访问
        mockMvc.perform(get("/api/tasks/dead-letters")).andExpect(status().isOk());
    }

    @Test
    void renewThenAbandon_thenTaskReclaimed_andCapacityEndpointExposed() throws Exception {
        String key = "web-ops-" + System.nanoTime();
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "\",\"payload\":\"p\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        String pollBody = mockMvc.perform(post("/api/tasks/poll").param("owner", "ops-w1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.state").value("RUNNING"))
                .andReturn().getResponse().getContentAsString();
        long offset = objectMapper.readTree(pollBody).get("offset").asLong();

        // 续期成功
        mockMvc.perform(post("/api/tasks/{offset}/renew", offset).param("owner", "ops-w1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("RENEWED"))
                .andExpect(jsonPath("$.newLeaseExpireAtMillis").isNumber());
        // 非占用者续期被拒
        mockMvc.perform(post("/api/tasks/{offset}/renew", offset).param("owner", "ops-w2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("LEASE_EXPIRED"));

        // 主动放弃：立即回到 PENDING 且原因可解释
        mockMvc.perform(post("/api/tasks/{offset}/abandon", offset)
                        .param("owner", "ops-w1").param("reason", "graceful shutdown"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("APPLIED"));
        mockMvc.perform(get("/api/tasks/{offset}", offset))
                .andExpect(jsonPath("$.state").value("PENDING"))
                .andExpect(jsonPath("$.pendingReason").value("ABANDONED"));

        // 原占用者的确认被拒绝（任务已解除占用回到 PENDING -> NOT_RUNNING）；他人可立即接手
        mockMvc.perform(post("/api/tasks/{offset}/ack", offset).param("owner", "ops-w1"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("NOT_RUNNING"));
        mockMvc.perform(post("/api/tasks/poll").param("owner", "ops-w2"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.ownerToken").value("ops-w2"))
                .andExpect(jsonPath("$.state").value("RUNNING"));

        mockMvc.perform(get("/api/tasks/capacity"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.capacity").value(0))
                .andExpect(jsonPath("$.currentSize").isNumber());
    }
}
