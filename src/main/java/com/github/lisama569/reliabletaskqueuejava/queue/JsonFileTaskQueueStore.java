package com.github.lisama569.reliabletaskqueuejava.queue;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 基于 JSON 文件的快照存储：先写临时文件再原子 move，保证快照始终完整可读。
 */
public class JsonFileTaskQueueStore implements TaskQueueStore {

    private static final Logger log = LoggerFactory.getLogger(JsonFileTaskQueueStore.class);

    private final Path path;
    private final ObjectMapper objectMapper;

    public JsonFileTaskQueueStore(Path path, ObjectMapper objectMapper) {
        this.path = path;
        this.objectMapper = objectMapper;
    }

    @Override
    public QueueSnapshot load() {
        if (!Files.exists(path)) {
            log.info("snapshot file not found, starting with empty queue: {}", path.toAbsolutePath());
            return null;
        }
        try {
            QueueSnapshot snapshot = objectMapper.readValue(path.toFile(), QueueSnapshot.class);
            log.info("snapshot loaded: file={}, tasks={}, committedOffset={}",
                    path.toAbsolutePath(),
                    snapshot.getTasks() == null ? 0 : snapshot.getTasks().size(),
                    snapshot.getCommittedOffset());
            return snapshot;
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load snapshot from " + path.toAbsolutePath(), e);
        }
    }

    @Override
    public void save(QueueSnapshot snapshot) {
        try {
            Path parent = path.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            Path tmp = path.resolveSibling(path.getFileName() + ".tmp");
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(tmp.toFile(), snapshot);
            try {
                Files.move(tmp, path, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException atomicNotSupported) {
                Files.move(tmp, path, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("failed to save snapshot to " + path.toAbsolutePath(), e);
        }
    }

    public Path getPath() {
        return path;
    }
}
