package org.adam.mq;

import org.adam.mq.common.Consumer;
import org.adam.mq.mqserver.VirtualHost;
import org.adam.mq.mqserver.core.BasicProperties;
import org.adam.mq.mqserver.core.ExchangeType;
import org.adam.mq.support.TestRuntimeSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@SpringBootTest
public class VirtualHostTest {
    private VirtualHost virtualHost;

    @BeforeEach
    public void setup() throws IOException {
        TestRuntimeSupport.deleteDataDirectory();
        TestRuntimeSupport.startApplicationContext();
        virtualHost = new VirtualHost("test_vhost");
    }

    @AfterEach
    public void teardown() throws IOException {
        TestRuntimeSupport.stopApplicationContext();
        TestRuntimeSupport.deleteDataDirectory();
        virtualHost = null;
    }

    @Test
    public void testExchangeDeclare() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testExchangeDelete() {
        boolean success = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(success);

        boolean successDelete = virtualHost.exchangeDelete("test_Exchange");
        Assertions.assertTrue(successDelete);
    }

    @Test
    public void testQueueDeclare() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueDelete() {
        boolean success = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(success);

        success = virtualHost.queueDelete("test_Queue");
        Assertions.assertTrue(success);
    }

    @Test
    public void testQueueBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);
    }

    @Test
    public void testQueueUnBind() {
        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        boolean queueUnBind = virtualHost.queueUnbind("test_Queue", "test_Exchange");
        Assertions.assertTrue(queueUnBind);
    }

    @Test
    public void testBasicPublish() {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "test_BindingKey");
        Assertions.assertTrue(queueBind);

        String message = "Hello, World!";
        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "test_BindingKey", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);
    }

    @Test
    public void testBasicConsume1() throws Exception {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedRoutingKey = new AtomicReference<>();
        AtomicReference<Integer> receivedDeliveryMode = new AtomicReference<>();
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();

        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                receivedRoutingKey.set(basicProperties.getRoutingKey());
                receivedDeliveryMode.set(basicProperties.getDeliveryMode());
                receivedBody.set(body);
                latch.countDown();
            }
        });
        Assertions.assertTrue(ok);

        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "consumer did not receive the message in time");
        Assertions.assertEquals("testQueue", receivedRoutingKey.get());
        Assertions.assertEquals(1, receivedDeliveryMode.get());
        Assertions.assertArrayEquals("hello".getBytes(), receivedBody.get());
    }

    @Test
    public void testBasicConsume2() throws Exception {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("test_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("test_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("test_Queue", "test_Exchange", "testQueue");
        Assertions.assertTrue(queueBind);

        boolean publishSuccess = virtualHost.basicPublish("test_Exchange", "testQueue", null, "hello".getBytes());
        Assertions.assertTrue(publishSuccess);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> receivedRoutingKey = new AtomicReference<>();
        AtomicReference<Integer> receivedDeliveryMode = new AtomicReference<>();
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();

        boolean ok = virtualHost.basicConsume("testConsumerTag", "test_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                receivedRoutingKey.set(basicProperties.getRoutingKey());
                receivedDeliveryMode.set(basicProperties.getDeliveryMode());
                receivedBody.set(body);
                latch.countDown();
            }
        });
        Assertions.assertTrue(ok);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "consumer did not receive the queued message in time");
        Assertions.assertEquals("testQueue", receivedRoutingKey.get());
        Assertions.assertEquals(1, receivedDeliveryMode.get());
        Assertions.assertArrayEquals("hello".getBytes(), receivedBody.get());
    }

    @Test
    public void testBasicConsumeFanout() throws Exception {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("fanout_Exchange", ExchangeType.FANOUT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare1 = virtualHost.queueDeclare("fanout_Queue1", true, false, false, null);
        Assertions.assertTrue(queueDeclare1);
        boolean queueDeclareBind1 = virtualHost.queueBind("fanout_Queue1", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind1);

        boolean queueDeclare2 = virtualHost.queueDeclare("fanout_Queue2", true, false, false, null);
        Assertions.assertTrue(queueDeclare2);
        boolean queueDeclareBind2 = virtualHost.queueBind("fanout_Queue2", "fanout_Exchange", "");
        Assertions.assertTrue(queueDeclareBind2);

        String message = "Hello, Fanout!";
        boolean publishSuccess = virtualHost.basicPublish("fanout_Exchange", "", null, message.getBytes());
        Assertions.assertTrue(publishSuccess);

        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<byte[]> body1 = new AtomicReference<>();
        AtomicReference<byte[]> body2 = new AtomicReference<>();

        boolean consumer1 = virtualHost.basicConsume("fanoutConsumerTag1", "fanout_Queue1", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                body1.set(body);
                latch.countDown();
            }
        });
        Assertions.assertTrue(consumer1);

        boolean consumer2 = virtualHost.basicConsume("fanoutConsumerTag2", "fanout_Queue2", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                body2.set(body);
                latch.countDown();
            }
        });
        Assertions.assertTrue(consumer2);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "fanout message did not reach both consumers in time");
        Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body1.get());
        Assertions.assertArrayEquals("Hello, Fanout!".getBytes(), body2.get());
    }

    @Test
    public void testBasicConsumeTopic() throws Exception {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("topic_Exchange", ExchangeType.TOPIC, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("topic_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("topic_Queue", "topic_Exchange", "user.*.update");
        Assertions.assertTrue(queueBind);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicInteger callCount = new AtomicInteger(0);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();
        AtomicReference<Throwable> asyncFailure = new AtomicReference<>();

        boolean consumer = virtualHost.basicConsume("topicConsumerTag", "topic_Queue", true, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                try {
                    callCount.incrementAndGet();
                    receivedBody.set(body);
                } catch (Throwable throwable) {
                    asyncFailure.compareAndSet(null, throwable);
                } finally {
                    latch.countDown();
                }
            }
        });
        Assertions.assertTrue(consumer);

        boolean publishSuccess1 = virtualHost.basicPublish("topic_Exchange", "user.123.update", null, "User update message".getBytes());
        Assertions.assertTrue(publishSuccess1);

        boolean publishSuccess2 = virtualHost.basicPublish("topic_Exchange", "user.123.delete", null, "User delete message".getBytes());
        Assertions.assertTrue(publishSuccess2);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "matched topic message was not delivered in time");
        assertNoAsyncFailure(asyncFailure);
        TestRuntimeSupport.assertConditionStaysTrue(
                () -> callCount.get() == 1,
                300,
                20,
                "non-matching topic message should not reach the consumer");
        Assertions.assertEquals(1, callCount.get(), "non-matching topic message should not reach the consumer");
        Assertions.assertArrayEquals("User update message".getBytes(), receivedBody.get());
    }

    @Test
    public void testQueueBindRejectsInlineWildcardBindingKey() {
        Assertions.assertTrue(virtualHost.exchangeDeclare("invalid_exchange", ExchangeType.TOPIC, true, false, null));
        Assertions.assertTrue(virtualHost.queueDeclare("invalid_queue", true, false, false, null));
        Assertions.assertFalse(virtualHost.queueBind("invalid_queue", "invalid_exchange", "order.a*"));
    }

    @Test
    public void testBasicPublishRejectsInvalidRoutingKey() {
        Assertions.assertTrue(virtualHost.exchangeDeclare("invalid_routing_exchange", ExchangeType.TOPIC, true, false, null));
        Assertions.assertFalse(virtualHost.basicPublish("invalid_routing_exchange", "order.*.created", null, "hello".getBytes()));
    }

    @Test
    public void testBasicPublishFailsWhenExchangeMissing() {
        boolean publishSuccess = virtualHost.basicPublish("missing_exchange", "order.created", null, "hello".getBytes());
        Assertions.assertFalse(publishSuccess);
    }

    @Test
    public void testBasicAckFailsWhenMessageMissing() {
        Assertions.assertTrue(virtualHost.queueDeclare("ack_queue", true, false, false, null));
        Assertions.assertFalse(virtualHost.basicAck("ack_queue", "missing-message-id"));
    }

    @Test
    public void testDurableMessageIsRemovedFromDiskAfterAck() throws Exception {
        Assertions.assertTrue(virtualHost.exchangeDeclare("durable_exchange", ExchangeType.DIRECT, true, false, null));
        Assertions.assertTrue(virtualHost.queueDeclare("durable_queue", true, false, false, null));
        Assertions.assertTrue(virtualHost.queueBind("durable_queue", "durable_exchange", "durable_queue"));

        BasicProperties basicProperties = new BasicProperties();
        basicProperties.setDeliveryMode(2);
        Assertions.assertTrue(virtualHost.basicPublish("durable_exchange", "durable_queue", basicProperties, "durable".getBytes()));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> messageIdHolder = new AtomicReference<>();
        AtomicReference<Boolean> ackResult = new AtomicReference<>();
        AtomicReference<Throwable> asyncFailure = new AtomicReference<>();

        Assertions.assertTrue(virtualHost.basicConsume("durable_consumer", "durable_queue", false,
                (consumerTag, properties, body) -> {
                    try {
                        messageIdHolder.set(properties.getMessageId());
                        ackResult.set(virtualHost.basicAck("durable_queue", properties.getMessageId()));
                    } catch (Throwable throwable) {
                        asyncFailure.compareAndSet(null, throwable);
                    } finally {
                        latch.countDown();
                    }
                }));

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "durable message was not consumed in time");
        assertNoAsyncFailure(asyncFailure);
        Assertions.assertNotNull(messageIdHolder.get());
        Assertions.assertTrue(ackResult.get(), "ack should succeed");
        Assertions.assertTrue(virtualHost.getDiskDataCenter()
                .loadAllMessageFromQueue("test_vhost-durable_queue")
                .isEmpty());
    }

    @Test
    public void testBasicAck() throws Exception {
        boolean exchangeDeclare = virtualHost.exchangeDeclare("ack_Exchange", ExchangeType.DIRECT, true, false, null);
        Assertions.assertTrue(exchangeDeclare);

        boolean queueDeclare = virtualHost.queueDeclare("ack_Queue", true, false, false, null);
        Assertions.assertTrue(queueDeclare);

        boolean queueBind = virtualHost.queueBind("ack_Queue", "ack_Exchange", "ack_BindingKey");
        Assertions.assertTrue(queueBind);

        boolean publishSuccess = virtualHost.basicPublish("ack_Exchange", "ack_BindingKey", null, "Message needing ack".getBytes());
        Assertions.assertTrue(publishSuccess);

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<byte[]> receivedBody = new AtomicReference<>();
        AtomicReference<Boolean> ackResult = new AtomicReference<>();
        AtomicReference<Throwable> asyncFailure = new AtomicReference<>();

        boolean consumer = virtualHost.basicConsume("ackConsumerTag", "ack_Queue", false, new Consumer() {
            @Override
            public void handleDelivery(String consumerTag, BasicProperties basicProperties, byte[] body) {
                try {
                    receivedBody.set(body);
                    ackResult.set(virtualHost.basicAck("ack_Queue", basicProperties.getMessageId()));
                } catch (Throwable throwable) {
                    asyncFailure.compareAndSet(null, throwable);
                } finally {
                    latch.countDown();
                }
            }
        });
        Assertions.assertTrue(consumer);

        Assertions.assertTrue(latch.await(3, TimeUnit.SECONDS), "ack message was not consumed in time");
        assertNoAsyncFailure(asyncFailure);
        Assertions.assertArrayEquals("Message needing ack".getBytes(), receivedBody.get());
        Assertions.assertTrue(ackResult.get(), "ack should succeed");
    }

    private void assertNoAsyncFailure(AtomicReference<Throwable> asyncFailure) {
        Throwable throwable = asyncFailure.get();
        if (throwable != null) {
            Assertions.fail(throwable);
        }
    }
}
