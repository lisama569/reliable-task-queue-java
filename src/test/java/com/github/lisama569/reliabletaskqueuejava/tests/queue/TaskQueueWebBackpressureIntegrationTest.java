package com.github.lisama569.reliabletaskqueuejava.tests.queue;

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

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * 端到端：HTTP 层容量背压——满员返回 429 与确定的 QUEUE_FULL；
 * 相同幂等键的重复提交即使在满员时也维持幂等结果（200 DEDUPLICATED）。
 *
 * <p>使用 {@link TempDir} 下的独立快照并将容量设为 1，避免与其他用例共享队列状态。
 */
@SpringBootTest
@AutoConfigureMockMvc
class TaskQueueWebBackpressureIntegrationTest {

    @TempDir
    static Path tempDir;

    @DynamicPropertySource
    static void props(DynamicPropertyRegistry registry) {
        registry.add("taskqueue.store-path", () -> tempDir.resolve("web-bp-snapshot.json").toString());
        registry.add("taskqueue.max-tasks", () -> 1);
    }

    @Autowired
    private MockMvc mockMvc;

    @Test
    void queueFullReturns429_andDuplicateStillIdempotent() throws Exception {
        String key = "web-bp-" + System.nanoTime();
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "\",\"payload\":\"p\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("ACCEPTED"));

        // 不同 key 的新提交：容量背压 -> 429 QUEUE_FULL，任务被明确拒绝而非静默丢弃
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "-other\",\"payload\":\"p2\"}"))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.status").value("QUEUE_FULL"));

        // 相同 key 重复提交：不占容量，幂等结果不被容量限制改变
        mockMvc.perform(post("/api/tasks")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content("{\"key\":\"" + key + "\",\"payload\":\"p-dup\"}"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("DEDUPLICATED"));
    }
}
