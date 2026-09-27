package com.github.highcumontoa.concurrentbatchpipelinejava.domain;

/**
 * 待处理记录。businessKey 为幂等键；payload 为业务负载。
 */
public record BatchRecord(String businessKey, String payload) {
}
