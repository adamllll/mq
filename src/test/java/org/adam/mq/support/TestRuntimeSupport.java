package org.adam.mq.support;

import org.adam.mq.MqApplication;
import org.adam.mq.mqserver.BrokerServer;
import org.apache.tomcat.util.http.fileupload.FileUtils;
import org.springframework.boot.SpringApplication;

import java.io.File;
import java.io.IOException;
import java.net.Socket;

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
        serverThread.start();
        return serverThread;
    }

    public static void awaitBrokerReady(int port) {
        long deadline = System.currentTimeMillis() + 3000;
        while (System.currentTimeMillis() < deadline) {
            try (Socket ignored = new Socket("127.0.0.1", port)) {
                return;
            } catch (IOException ignored) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("等待 Broker 启动时被中断", e);
                }
            }
        }
        throw new IllegalStateException("Broker 未在 3 秒内启动完成，port=" + port);
    }
}
