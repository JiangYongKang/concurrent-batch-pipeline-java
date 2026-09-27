package com.github.highcumontoa.concurrentbatchpipelinejava.service;

import com.github.highcumontoa.concurrentbatchpipelinejava.domain.RecordStatus;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 管线计数器。守恒不变量：
 * submitted == 各终态计数之和 + inFlight。
 */
@Component
public class PipelineMetrics {

    private final AtomicLong submitted = new AtomicLong();
    private final AtomicLong inFlight = new AtomicLong();
    private final Map<RecordStatus, AtomicLong> terminalCounts = new ConcurrentHashMap<>();

    public void onSubmitted() {
        submitted.incrementAndGet();
        inFlight.incrementAndGet();
    }

    public void onTerminal(RecordStatus status) {
        terminalCounts.computeIfAbsent(status, k -> new AtomicLong()).incrementAndGet();
        inFlight.decrementAndGet();
    }

    public long submitted() {
        return submitted.get();
    }

    public long inFlight() {
        return inFlight.get();
    }

    public long terminalTotal() {
        return terminalCounts.values().stream().mapToLong(AtomicLong::get).sum();
    }

    /** 守恒校验：submitted == terminalTotal + inFlight。 */
    public boolean isConserved() {
        return submitted() == terminalTotal() + inFlight();
    }

    public Map<String, Long> snapshot() {
        Map<String, Long> snap = new ConcurrentHashMap<>();
        snap.put("submitted", submitted());
        snap.put("inFlight", inFlight());
        terminalCounts.forEach((k, v) -> snap.put(k.name(), v.get()));
        snap.put("conserved", isConserved() ? 1L : 0L);
        return snap;
    }
}
