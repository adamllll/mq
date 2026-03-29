package org.adam.mq.perf;

public record PerformanceStats(String scenario, int totalMessages, int successCount, int failureCount, long elapsedNanos) {
    public double throughputPerSecond() {
        if (elapsedNanos == 0L) {
            return 0.0;
        }
        return successCount / (elapsedNanos / 1_000_000_000.0);
    }

    public double averageLatencyMillis() {
        if (successCount == 0) {
            return 0.0;
        }
        return (elapsedNanos / 1_000_000.0) / successCount;
    }

    public String summary() {
        return String.format(
                "%s | total=%d | success=%d | failure=%d | elapsedMs=%.2f | tps=%.2f | avgLatencyMs=%.2f",
                scenario,
                totalMessages,
                successCount,
                failureCount,
                elapsedNanos / 1_000_000.0,
                throughputPerSecond(),
                averageLatencyMillis()
        );
    }
}
