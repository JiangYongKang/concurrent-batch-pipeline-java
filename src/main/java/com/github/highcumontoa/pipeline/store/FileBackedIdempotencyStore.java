package com.github.highcumontoa.pipeline.store;

import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
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
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地追加日志文件的幂等存储。
 *
 * <p>语义：
 * <ul>
 *   <li>{@code tryClaim} 仅首次见到某 businessId 时成功，保证处理副作用只发生一次；</li>
 *   <li>终态结果先落盘（append + flush）再对调用方可见，重启后已确认结果不丢失；</li>
 *   <li>重启时从日志文件回放终态结果；在途（已 claim 未落终态）的标识不持久化，
 *       重启后允许重新提交，不会产生重复副作用，因为副作用只在终态落盘前发生一次。</li>
 * </ul>
 */
@Component
public class FileBackedIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(FileBackedIdempotencyStore.class);

    private final Path file;
    /** 已占位（处理中或已完成）的标识。 */
    private final Map<String, Boolean> claimed = new ConcurrentHashMap<>();
    /** 已确认的终态结果。 */
    private final Map<String, RecordResult> results = new ConcurrentHashMap<>();

    public FileBackedIdempotencyStore(@Value("${pipeline.store-file:data/idempotency-store.log}") String storeFile) {
        this.file = Path.of(storeFile);
    }

    @PostConstruct
    public void load() {
        try {
            if (file.getParent() != null) {
                Files.createDirectories(file.getParent());
            }
            if (!Files.exists(file)) {
                Files.createFile(file);
                log.info("idempotency store created at {}", file.toAbsolutePath());
                return;
            }
            long loaded = 0;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                RecordResult r = decode(line);
                if (r != null) {
                    results.put(r.businessId(), r);
                    claimed.put(r.businessId(), Boolean.TRUE);
                    loaded++;
                }
            }
            log.info("idempotency store reloaded {} confirmed results from {}", loaded, file.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load idempotency store " + file, e);
        }
    }

    @Override
    public boolean tryClaim(String businessId) {
        return claimed.putIfAbsent(businessId, Boolean.TRUE) == null;
    }

    @Override
    public void saveResult(RecordResult result) {
        // 先持久化，再暴露给查询，保证"已确认结果不丢失"。
        append(encode(result));
        results.put(result.businessId(), result);
    }

    @Override
    public Optional<RecordResult> find(String businessId) {
        return Optional.ofNullable(results.get(businessId));
    }

    @Override
    public long count() {
        return results.size();
    }

    private synchronized void append(String line) {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            w.write(line);
            w.newLine();
            w.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist result to " + file, e);
        }
    }

    private static String encode(RecordResult r) {
        return esc(r.businessId()) + '\t' + r.status().name() + '\t' + r.attempts() + '\t' + esc(r.reason());
    }

    private static RecordResult decode(String line) {
        String[] parts = line.split("\t", -1);
        if (parts.length != 4) {
            return null;
        }
        try {
            return new RecordResult(unesc(parts[0]), RecordStatus.valueOf(parts[1]),
                    parts[3].isEmpty() ? "" : unesc(parts[3]), Integer.parseInt(parts[2]));
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    private static String esc(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("\\", "\\\\").replace("\t", "\\t").replace("\n", "\\n");
    }

    private static String unesc(String s) {
        StringBuilder sb = new StringBuilder(s.length());
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            if (c == '\\' && i + 1 < s.length()) {
                char n = s.charAt(++i);
                sb.append(n == 't' ? '\t' : n == 'n' ? '\n' : n);
            } else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
}
