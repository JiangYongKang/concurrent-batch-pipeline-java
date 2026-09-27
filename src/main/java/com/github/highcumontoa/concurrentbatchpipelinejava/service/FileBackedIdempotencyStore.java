package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.config.PipelineProperties;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordResult;
import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordStatus;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地追加日志文件的幂等存储。
 *
 * <p>持久化模型（WAL，逐行追加、每次写后 flush）：
 * <ul>
 *   <li>{@code C\tkey} —— 受理登记（claim），先于任何处理副作用落盘；</li>
 *   <li>{@code F\tkey\tSTATUS\tattempts\turlenc(reason)} —— 终态结果。</li>
 * </ul>
 * 重启恢复时：仅有 C 而无 F 的键视为"停机时在途、结果未知"，保持已占用状态，
 * 对重复提交返回 DUPLICATE 而不重新处理，从而保证不产生重复副作用、已确认终态不丢失。
 */
@Component
public class FileBackedIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(FileBackedIdempotencyStore.class);

    private final Path storePath;
    /** 处理中的键（已受理未终态）。 */
    private final Set<String> claimed = ConcurrentHashMap.newKeySet();
    /** 终态结果。 */
    private final Map<String, RecordResult> terminal = new ConcurrentHashMap<>();
    /** 写文件锁：保证追加行不交错。 */
    private final Object writeLock = new Object();
    private BufferedWriter writer;

    @org.springframework.beans.factory.annotation.Autowired
    public FileBackedIdempotencyStore(PipelineProperties properties) {
        this(properties.storePath());
    }

    public FileBackedIdempotencyStore(String storePath) {
        this.storePath = Path.of(storePath);
        recover();
        openWriter();
    }

    private void recover() {
        if (!Files.exists(storePath)) {
            return;
        }
        try (var lines = Files.lines(storePath, StandardCharsets.UTF_8)) {
            lines.forEach(line -> {
                String[] parts = line.split("\t", -1);
                if (parts.length >= 2 && "C".equals(parts[0])) {
                    claimed.add(parts[1]);
                } else if (parts.length == 5 && "F".equals(parts[0])) {
                    RecordResult r = new RecordResult(parts[1], RecordStatus.valueOf(parts[2]),
                            decode(parts[4]), Integer.parseInt(parts[3]));
                    terminal.put(parts[1], r);
                    claimed.remove(parts[1]);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException("恢复幂等存储失败: " + storePath, e);
        }
        log.info("幂等存储恢复完成: claimed={} terminal={} path={}", claimed.size(), terminal.size(), storePath);
    }

    private void openWriter() {
        try {
            Files.createDirectories(storePath.toAbsolutePath().getParent());
            writer = Files.newBufferedWriter(storePath, StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new UncheckedIOException("打开幂等存储失败: " + storePath, e);
        }
    }

    private void append(String line) {
        synchronized (writeLock) {
            try {
                writer.write(line);
                writer.newLine();
                writer.flush();
            } catch (IOException e) {
                throw new UncheckedIOException("写入幂等存储失败", e);
            }
        }
    }

    @Override
    public boolean tryClaim(String businessKey) {
        synchronized (writeLock) {
            if (claimed.contains(businessKey) || terminal.containsKey(businessKey)) {
                return false;
            }
            append("C\t" + businessKey);
            claimed.add(businessKey);
            return true;
        }
    }

    @Override
    public void complete(String businessKey, RecordResult result) {
        synchronized (writeLock) {
            if (!claimed.contains(businessKey)) {
                return;
            }
            append("F\t" + businessKey + "\t" + result.status().name()
                    + "\t" + result.attempts() + "\t" + encode(result.reason()));
            claimed.remove(businessKey);
            terminal.put(businessKey, result);
        }
    }

    @Override
    public Optional<RecordResult> findResult(String businessKey) {
        return Optional.ofNullable(terminal.get(businessKey));
    }

    @Override
    public int size() {
        return claimed.size() + terminal.size();
    }

    @PreDestroy
    public void close() {
        synchronized (writeLock) {
            try {
                if (writer != null) {
                    writer.flush();
                    writer.close();
                }
            } catch (IOException e) {
                log.warn("关闭幂等存储失败: {}", e.toString());
            }
        }
    }

    private static String encode(String s) {
        return java.net.URLEncoder.encode(s == null ? "" : s, StandardCharsets.UTF_8);
    }

    private static String decode(String s) {
        return java.net.URLDecoder.decode(s, StandardCharsets.UTF_8);
    }
}
