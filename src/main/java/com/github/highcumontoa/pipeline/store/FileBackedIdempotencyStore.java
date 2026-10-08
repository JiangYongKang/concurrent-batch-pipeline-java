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
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地追加日志文件的幂等存储。
 *
 * <p>两类内部记录严格分离：
 * <ul>
 *   <li><b>在途占位 {@code inFlight}</b>：仅存内存，表示"已受理、尚无最终结论"。
 *       不持久化——服务中断后这类标识没有任何确定结论，重启后允许重新提交受理，
 *       不会出现"嘴上说处理过、其实没处理"。</li>
 *   <li><b>最终结论 {@code results}</b>：每条先 append + flush 落盘再对查询可见，
 *       重启时回放日志恢复，已对外给过的确定结论一条不丢。</li>
 * </ul>
 *
 * <p>背压快速拒绝发生在占位之后、处理之前，{@link #releaseClaim(String)} 会把对应
 * 在途占位收回，同一 businessId 随后可作为新任务正常受理；该回收不触碰任何最终结论，
 * 因此系统内部占用不会随拒绝次数只增不减。
 */
@Component
public class FileBackedIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(FileBackedIdempotencyStore.class);

    private final Path file;
    /** 在途占位（处理中、尚无最终结论），仅内存，不持久化。 */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    /** 已确认的最终结果，落盘持久化，重启回放。以先到为准，不覆盖。 */
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
                if (r != null && results.putIfAbsent(r.businessId(), r) == null) {
                    loaded++;
                }
            }
            // 重启不恢复任何在途占位：中断时没有最终结论的标识一律允许重新提交。
            log.info("idempotency store reloaded {} confirmed results from {} (in-flight claims reset)",
                    loaded, file.toAbsolutePath());
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load idempotency store " + file, e);
        }
    }

    @Override
    public boolean tryClaim(String businessId) {
        // 已有最终结论：视为重复，不再受理。
        if (results.containsKey(businessId)) {
            return false;
        }
        // 同一标识在途占位唯一；已有占位（在途）则拒绝。
        return inFlight.add(businessId);
    }

    @Override
    public boolean releaseClaim(String businessId) {
        // 安全护栏：最终结论优先，绝不能回收掉已确认结论。
        if (results.containsKey(businessId)) {
            return false;
        }
        boolean removed = inFlight.remove(businessId);
        if (!removed) {
            log.warn("releaseClaim for {} found no in-flight claim (terminal result or never claimed)", businessId);
        }
        return removed;
    }

    @Override
    public void saveResult(RecordResult result) {
        // 先持久化，再暴露给查询，保证"已确认结果不丢失"。
        append(encode(result));
        RecordResult previous = results.putIfAbsent(result.businessId(), result);
        if (previous != null) {
            // 正常路径不会发生：tryClaim 保证同一标识只有一个执行体。
            log.error("refuse to overwrite terminal result for {}: existing={}, incoming={}",
                    result.businessId(), previous, result);
        }
        inFlight.remove(result.businessId());
    }

    @Override
    public Optional<RecordResult> find(String businessId) {
        return Optional.ofNullable(results.get(businessId));
    }

    @Override
    public boolean isInFlight(String businessId) {
        return inFlight.contains(businessId) && !results.containsKey(businessId);
    }

    @Override
    public long count() {
        return results.size();
    }

    @Override
    public long claimedCount() {
        return results.size() + inFlight.size();
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
