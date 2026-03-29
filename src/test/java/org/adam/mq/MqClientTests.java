package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.common.MqException;
import org.adam.mq.mqclient.Channel;
import org.adam.mq.mqclient.Connection;
import org.adam.mq.mqclient.ConnectionFactory;
import org.adam.mq.mqserver.BrokerServer;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.support.TestRuntimeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class MqClientTests {
    private BrokerServer brokerServer;
    private ConnectionFactory connectionFactory;
    private Thread serverThread;

    @BeforeEach
    public void setUp() throws IOException {
        TestRuntimeSupport.deleteDataDirectory();
        TestRuntimeSupport.startApplicationContext();
        brokerServer = new BrokerServer(9090);
        serverThread = TestRuntimeSupport.startBroker(brokerServer);
        TestRuntimeSupport.awaitBrokerReady(9090);

        connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("127.0.0.1");
        connectionFactory.setPort(9090);
    }

    @AfterEach
    public void tearDown() throws IOException {
        if (brokerServer != null) {
            brokerServer.stop();
        }
        if (serverThread != null && serverThread.isAlive()) {
            serverThread.interrupt();
            TestRuntimeSupport.awaitThreadStopped(serverThread, 1000);
        }
        TestRuntimeSupport.stopApplicationContext();
        TestRuntimeSupport.deleteDataDirectory();
        connectionFactory = null;
    }

    @Test
    public void testConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);
        connection.close();
    }

    @Test
    public void testChannel() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testExchange() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success = channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.exchangeDelete("test_exchange");
        Assertions.assertTrue(success);

        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testQueue() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDelete("test_queue");
        Assertions.assertTrue(success);

        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testBinding() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success = channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueBind("test_queue", "test_exchange", "test_bindingkey");
        Assertions.assertTrue(success);

        success = channel.queueUnbind("test_queue", "test_exchange");
        Assertions.assertTrue(success);

        success = channel.queueDelete("test_queue");
        Assertions.assertTrue(success);

        success = channel.exchangeDelete("test_exchange");
        Assertions.assertTrue(success);

        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testMessage() throws IOException, MqException, InterruptedException {
        Connection connection = connectionFactory.newConnection();
        Assertions.assertNotNull(connection);

        Channel channel = connection.createChannel();
        Assertions.assertNotNull(channel);

        boolean success = channel.exchangeDeclare("test_exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        success = channel.queueDeclare("test_queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = channel.queueBind("test_queue", "test_exchange", "test_queue");
        Assertions.assertTrue(success);

        byte[] requestBody = "Hello Mq Server".getBytes();
        success = channel.basicPublish("test_exchange", "test_queue", null, requestBody);
        Assertions.assertTrue(success);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();

        success = channel.basicConsume("test_queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties properties, byte[] body) {
                receivedBody.set(body);
                latch.countDown();
            }
        });
        Assertions.assertTrue(success);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "消息未在超时内到达 consumer");
        Assertions.assertArrayEquals(requestBody, receivedBody.get());

        channel.queueUnbind("test_queue", "test_exchange");
        channel.queueDelete("test_queue");
        channel.exchangeDelete("test_exchange");
        channel.closeChannel();
        connection.close();
    }

    @Test
    public void testMultipleChannelsShareOneConnection() throws IOException {
        Connection connection = connectionFactory.newConnection();
        Channel first = connection.createChannel();
        Channel second = connection.createChannel();

        Assertions.assertNotNull(first);
        Assertions.assertNotNull(second);
        Assertions.assertNotEquals(first.getChannelId(), second.getChannelId());

        first.closeChannel();
        second.closeChannel();
        connection.close();
    }

    @Test
    public void testBasicConsumeRejectsSecondConsumerOnSameChannel() throws IOException, MqException {
        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        Assertions.assertTrue(channel.exchangeDeclare("consume_exchange", ExchangeType.DIRECT, true, false, null));
        Assertions.assertTrue(channel.queueDeclare("consume_queue", true, false, false, null));
        Assertions.assertTrue(channel.queueBind("consume_queue", "consume_exchange", "consume_queue"));
        Assertions.assertTrue(channel.basicConsume("consume_queue", true, (consumerTag, properties, body) -> {
        }));
        Assertions.assertThrows(MqException.class, () ->
                channel.basicConsume("consume_queue", true, (consumerTag, properties, body) -> {
                }));

        channel.closeChannel();
        connection.close();
    }
}
