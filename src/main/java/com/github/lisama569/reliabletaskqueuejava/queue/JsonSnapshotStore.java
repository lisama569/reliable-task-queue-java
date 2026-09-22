package com.github.lisama569.reliabletaskqueuejava.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于本地 JSON 文件的快照存储，用进程内存储 + 本地落盘模拟消息代理的持久化。
 *
 * <p>写入采用“写临时文件 + 原子 rename”，保证崩溃时要么读到旧快照要么读到新快照，
 * 不会出现半截文件。快照在每次状态变更后同步覆盖写。
 */
public class JsonSnapshotStore implements SnapshotStore {

    private static final Logger log = LoggerFactory.getLogger(JsonSnapshotStore.class);

    private final Path file;
    private final ObjectMapper mapper;

    public JsonSnapshotStore(Path file) {
        this.file = file;
        this.mapper = new ObjectMapper()
                .enable(SerializationFeature.INDENT_OUTPUT)
                .findAndRegisterModules();
    }

    @Override
    public QueueSnapshot load() {
        if (file == null || !Files.exists(file)) {
            return null;
        }
        try {
            QueueSnapshot snapshot = mapper.readValue(Files.readAllBytes(file), QueueSnapshot.class);
            log.info("[snapshot] 加载快照成功: file={}, tasks={}, nextId={}, committedOffset={}",
                    file, snapshot.getTasks().size(), snapshot.getNextId(), snapshot.getCommittedOffset());
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("加载队列快照失败: " + file, e);
        }
    }

    @Override
    public synchronized void save(QueueSnapshot snapshot) {
        if (file == null) {
            return;
        }
        try {
            Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            byte[] bytes = mapper.writeValueAsBytes(snapshot);
            Files.write(tmp, bytes);
            try {
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING,
                        StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException atomicFailed) {
                // 某些文件系统不支持 ATOMIC_MOVE，退回普通覆盖 rename。
                Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("持久化队列快照失败: " + file, e);
        }
    }
}
