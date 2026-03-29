package org.adam.mq.support;

import org.adam.mq.MqApplication;
import org.adam.mq.mqserver.BrokerServer;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.function.BooleanSupplier;

public final class TestRuntimeSupport {
    private TestRuntimeSupport() {
    }

    public static void startApplicationContext() {
        if (MqApplication.context == null || !MqApplication.context.isActive()) {
            MqApplication.context = SpringApplication.run(MqApplication.class);
        }
    }

    public static void stopApplicationContext() {
        if (MqApplication.context != null && MqApplication.context.isActive()) {
            MqApplication.context.close();
        }
        MqApplication.context = null;
    }

    public static void deleteDataDirectory() throws IOException {
        File dataDir = new File("./data");
        if (dataDir.exists()) {
            FileUtils.deleteDirectory(dataDir);
        }
    }

    public static Thread startBroker(BrokerServer brokerServer) {
        Thread serverThread = new Thread(() -> {
            try {
                brokerServer.start();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }, "mq-broker-test-thread");
        serverThread.setDaemon(true);
        serverThread.start();
        return serverThread;
    }

    public static void awaitBrokerReady(int port) {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket("127.0.0.1", port)) {
                return;
            } catch (IOException ignored) {
                sleep(50, "等待 Broker 启动时被中断");
            }
        }
        throw new IllegalStateException("Broker 未在 3 秒内启动完成，port=" + port);
    }

    public static void awaitThreadStopped(Thread thread, long timeoutMillis) {
        if (thread == null) {
            return;
        }
        try {
            thread.join(timeoutMillis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("等待 Broker 测试线程退出时被中断", e);
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Broker 测试线程未在 " + timeoutMillis + "ms 内退出");
        }
    }

    public static void assertConditionStaysTrue(BooleanSupplier condition,
                                                long durationMillis,
                                                long pollIntervalMillis,
                                                String failureMessage) {
        long deadline = System.currentTimeMillis() + durationMillis;
        while (System.currentTimeMillis() < deadline) {
            if (!condition.getAsBoolean()) {
                throw new AssertionError(failureMessage);
            }
            sleep(pollIntervalMillis, "等待异步条件稳定时被中断");
        }
    }

    private static void sleep(long millis, String interruptedMessage) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(interruptedMessage, e);
        }
    }
}
