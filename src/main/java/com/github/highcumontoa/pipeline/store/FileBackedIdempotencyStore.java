package com.github.highcumontoa.pipeline.store;

import com.github.highcumontoa.pipeline.core.RecordResult;
import com.github.highcumontoa.pipeline.core.RecordSnapshot;
import com.github.highcumontoa.pipeline.core.RecordStatus;
import com.github.highcumontoa.pipeline.core.SubmissionState;
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
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地追加日志（WAL）的幂等存储。
 *
 * <p>日志行格式（制表符分隔）：
 * <ul>
 *   <li>{@code C\t<businessId>}：一次处理尝试已开始（claim）；</li>
 *   <li>{@code R\t<businessId>\t<status>\t<attempts>\t<reason>}：终态结果；</li>
 *   <li>无前缀的旧格式四字段行按终态结果兼容回放。</li>
 * </ul>
 *
 * <p>回放规则（严格按行序）：
 * <ul>
 *   <li>见到 C 而之后没有对应 R 的标识 → INTERRUPTED（处理途中被中断，结论未定）；</li>
 *   <li>见到 R 的标识 → CONFIRMED，终态以最后一条 R 为准；</li>
 *   <li>容量拒绝只回滚内存占位、不写日志，回放/长跑中都不留痕。</li>
 * </ul>
 *
 * <p>占用回收：{@code states} 只保存在途/中断标识，终态落盘或回滚后即移除；
 * {@code pendingClaims} 只保留"有 C 无 R"的标识，终态落盘后同步移除，
 * 因此反复被容量挡回不会让内部记录只增不减。
 */
@Component
public class FileBackedIdempotencyStore implements IdempotencyStore {

    private static final Logger log = LoggerFactory.getLogger(FileBackedIdempotencyStore.class);

    private final Path file;
    /** 非终态标识：值为 IN_FLIGHT（本进程在途）或 INTERRUPTED（回放/故障遗留）。 */
    private final ConcurrentHashMap<String, SubmissionState> states = new ConcurrentHashMap<>();
    /** 已确认的终态结果（只增不减，重启回放恢复）。 */
    private final ConcurrentHashMap<String, RecordResult> results = new ConcurrentHashMap<>();
    /** 有 C 行但尚无 R 行的标识集合（= INTERRUPTED 集合），终态落盘后移除。 */
    private final Set<String> pendingClaims = ConcurrentHashMap.newKeySet();

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
            long confirmed = 0;
            long interrupted = 0;
            for (String line : Files.readAllLines(file, StandardCharsets.UTF_8)) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("C\t")) {
                    String id = unesc(line.substring(2));
                    pendingClaims.add(id);
                    states.put(id, SubmissionState.INTERRUPTED);
                } else {
                    RecordResult r = decode(line.startsWith("R\t") ? line.substring(2) : line);
                    if (r != null) {
                        results.put(r.businessId(), r);
                        states.remove(r.businessId());
                        pendingClaims.remove(r.businessId());
                        confirmed++;
                    }
                }
            }
            interrupted = pendingClaims.size();
            log.info("idempotency store reloaded from {}: confirmed={}, interrupted(no conclusion)={}",
                    file.toAbsolutePath(), confirmed, interrupted);
        } catch (IOException e) {
            throw new UncheckedIOException("failed to load idempotency store " + file, e);
        }
    }

    @Override
    public ClaimOutcome acquireClaim(String businessId) {
        // 单次 compute 内完成"查终态 + 查在途/中断 + 占位"，并发提交下判定原子。
        ClaimOutcome[] outcome = new ClaimOutcome[1];
        states.compute(businessId, (id, prev) -> {
            if (results.containsKey(id)) {
                outcome[0] = ClaimOutcome.ALREADY_CONFIRMED;
                return prev; // 不改变状态
            }
            if (prev == SubmissionState.IN_FLIGHT) {
                outcome[0] = ClaimOutcome.ALREADY_IN_FLIGHT;
                return prev; // 本进程在途，去重
            }
            // prev == null（从未见过 / 仅被容量拒绝并回滚）
            // 或 prev == INTERRUPTED（上次处理未见终态）：都作为新尝试受理。
            outcome[0] = ClaimOutcome.ACCEPTED;
            return SubmissionState.IN_FLIGHT;
        });
        return outcome[0];
    }

    @Override
    public void rollbackClaim(String businessId) {
        states.computeIfPresent(businessId, (id, prev) -> {
            if (prev != SubmissionState.IN_FLIGHT) {
                return prev;
            }
            // 有持久化的 C 无 R：恢复为中断未决；否则彻底回收，回到 NOT_FOUND。
            return pendingClaims.contains(id) ? SubmissionState.INTERRUPTED : null;
        });
    }

    @Override
    public void persistAttemptStarted(String businessId) {
        // 先持久化再更新内存：即便崩溃，重启回放也能识别出这次未完成的尝试。
        append("C\t" + esc(businessId));
        pendingClaims.add(businessId);
    }

    @Override
    public void markInterrupted(String businessId) {
        states.computeIfPresent(businessId, (id, prev) ->
                prev == SubmissionState.IN_FLIGHT
                        ? (pendingClaims.contains(id) ? SubmissionState.INTERRUPTED : null)
                        : prev);
    }

    @Override
    public void saveResult(RecordResult result) {
        // 先持久化，再暴露给查询，保证"已确认结论不丢失"。
        append("R\t" + encode(result));
        results.put(result.businessId(), result);
        states.remove(result.businessId());
        pendingClaims.remove(result.businessId());
    }

    @Override
    public Optional<RecordResult> find(String businessId) {
        return Optional.ofNullable(results.get(businessId));
    }

    @Override
    public RecordSnapshot lookup(String businessId) {
        RecordResult r = results.get(businessId);
        if (r != null) {
            return new RecordSnapshot(businessId, SubmissionState.CONFIRMED,
                    r.status(), r.reason(), r.attempts());
        }
        SubmissionState s = states.get(businessId);
        if (s == SubmissionState.IN_FLIGHT) {
            return new RecordSnapshot(businessId, SubmissionState.IN_FLIGHT, null,
                    "submission accepted, processing in-flight, no conclusion yet", 0);
        }
        if (s == SubmissionState.INTERRUPTED) {
            return new RecordSnapshot(businessId, SubmissionState.INTERRUPTED, null,
                    "a previous attempt started but no conclusion was recorded "
                            + "(likely interrupted by shutdown/crash); resubmission is accepted", 0);
        }
        return new RecordSnapshot(businessId, SubmissionState.NOT_FOUND, null,
                "no submission known for this businessId "
                        + "(never submitted, or only capacity-rejected and reclaimed)", 0);
    }

    @Override
    public long count() {
        return results.size();
    }

    @Override
    public long inFlightCount() {
        return states.values().stream().filter(SubmissionState.IN_FLIGHT::equals).count();
    }

    @Override
    public long interruptedCount() {
        // 注意：已开始处理的在途标识也在 pendingClaims 中，但中断只以状态表中的
        // INTERRUPTED 为准（C 已写、本进程无在途执行体、且无 R）。
        return states.values().stream().filter(SubmissionState.INTERRUPTED::equals).count();
    }

    private synchronized void append(String line) {
        try (BufferedWriter w = Files.newBufferedWriter(file, StandardCharsets.UTF_8,
                StandardOpenOption.APPEND)) {
            w.write(line);
            w.newLine();
            w.flush();
        } catch (IOException e) {
            throw new UncheckedIOException("failed to persist to " + file, e);
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
