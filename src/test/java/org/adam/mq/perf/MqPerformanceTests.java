package org.adam.mq.perf;

import org.adam.mq.mqclient.Channel;
import org.adam.mq.mqclient.Connection;
import org.adam.mq.mqclient.ConnectionFactory;
import org.adam.mq.mqserver.BrokerServer;
import org.adam.mq.mqserver.VirtualHost;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.support.TestRuntimeSupport;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@EnabledIfSystemProperty(named = "mq.perf.enabled", matches = "true")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class MqPerformanceTests {
    private static BrokerServer brokerServer;
    private static Thread serverThread;
    private static ConnectionFactory connectionFactory;
    private static final List<PerformanceStats> RESULTS = new ArrayList<>();

    @BeforeAll
    static void beforeAll() throws Exception {
        TestRuntimeSupport.deleteDataDirectory();
        TestRuntimeSupport.startApplicationContext();
        brokerServer = new BrokerServer(9090);
        serverThread = TestRuntimeSupport.startBroker(brokerServer);
        TestRuntimeSupport.awaitBrokerReady(9090);

        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
    }

    @AfterAll
    static void afterAll() throws Exception {
        Path outputDir = Path.of("target", "perf-results");
        Files.createDirectories(outputDir);
        Files.writeString(outputDir.resolve("mq-performance-summary.txt"),
                String.join(System.lineSeparator(), RESULTS.stream().map(PerformanceStats::summary).toList()));

        if (brokerServer != null) {
            brokerServer.stop();
        }
        if (serverThread != null && serverThread.isAlive()) {
            serverThread.interrupt();
        }
        TestRuntimeSupport.stopApplicationContext();
        TestRuntimeSupport.deleteDataDirectory();
    }

    @Test
    @Order(1)
    void singleProducerThroughput() throws Exception {
        RESULTS.add(runPublishScenario("single-producer", 1, 500));
    }

    @Test
    @Order(2)
    void multiProducerThroughput() throws Exception {
        RESULTS.add(runPublishScenario("multi-producer", 4, 250));
    }

    @Test
    @Order(3)
    void publishConsumeAckLatency() throws Exception {
        VirtualHost virtualHost = new VirtualHost("perf_latency_vhost");
        int messageCount = 100;
        CountDownLatch latch = new CountDownLatch(messageCount);

        Assertions.assertTrue(virtualHost.exchangeDeclare("perf_latency_exchange", ExchangeType.DIRECT, true, false, null));
        Assertions.assertTrue(virtualHost.queueDeclare("perf_latency_queue", true, false, false, null));
        Assertions.assertTrue(virtualHost.queueBind("perf_latency_queue", "perf_latency_exchange", "perf_latency_queue"));
        Assertions.assertTrue(virtualHost.basicConsume("perf_latency_consumer", "perf_latency_queue", false, (consumerTag, properties, body) -> {
            Assertions.assertTrue(virtualHost.basicAck("perf_latency_queue", properties.getMessageId()));
            latch.countDown();
        }));

        long start = System.nanoTime();
        for (int i = 0; i < messageCount; i++) {
            BasicProperties properties = new BasicProperties();
            properties.setDeliveryMode(2);
            Assertions.assertTrue(virtualHost.basicPublish("perf_latency_exchange", "perf_latency_queue", properties,
                    ("latency-" + i).getBytes()));
        }
        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        long elapsed = System.nanoTime() - start;
        RESULTS.add(new PerformanceStats("publish-consume-ack", messageCount, messageCount, 0, elapsed));
    }

    private PerformanceStats runPublishScenario(String scenario, int producerThreads, int messagesPerThread) throws Exception {
        Connection setupConnection = connectionFactory.newConnection();
        Channel setupChannel = setupConnection.createChannel();
        String scenarioKey = scenario.replace('-', '_');
        String exchangeName = "perf_" + scenarioKey + "_exchange";
        String queueName = "perf_" + scenarioKey + "_queue";

        Assertions.assertTrue(setupChannel.exchangeDeclare(exchangeName, ExchangeType.DIRECT, true, false, null));
        Assertions.assertTrue(setupChannel.queueDeclare(queueName, true, false, false, null));
        Assertions.assertTrue(setupChannel.queueBind(queueName, exchangeName, queueName));
        setupChannel.closeChannel();
        setupConnection.close();

        AtomicInteger success = new AtomicInteger();
        AtomicInteger failure = new AtomicInteger();
        ExecutorService executor = Executors.newFixedThreadPool(producerThreads);
        CountDownLatch latch = new CountDownLatch(producerThreads);
        long start = System.nanoTime();

        for (int threadIndex = 0; threadIndex < producerThreads; threadIndex++) {
            executor.submit(() -> {
                try {
                    Connection connection = connectionFactory.newConnection();
                    Channel channel = connection.createChannel();
                    for (int i = 0; i < messagesPerThread; i++) {
                        boolean ok = channel.basicPublish(exchangeName, queueName, null,
                                ("payload-" + Thread.currentThread().getId() + "-" + i).getBytes());
                        if (ok) {
                            success.incrementAndGet();
                        } else {
                            failure.incrementAndGet();
                        }
                    }
                    channel.closeChannel();
                    connection.close();
                } catch (Exception e) {
                    failure.incrementAndGet();
                } finally {
                    latch.countDown();
                }
            });
        }

        Assertions.assertTrue(latch.await(10, TimeUnit.SECONDS));
        executor.shutdownNow();
        long elapsed = System.nanoTime() - start;
        return new PerformanceStats(scenario, producerThreads * messagesPerThread, success.get(), failure.get(), elapsed);
    }
}
