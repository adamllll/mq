package org.adam.mq.common;

/**
 * 表示一个消费者(完整的执行环境)
 */
public class ConsumerEnv {
    private String consumerTag; // 消费者标签
    private String queueName;    // 队列名称
    private boolean autoAck;     // 是否自动确认
    private Consumer consumer;   // 消费者回调接口,通过这个回调来处理收到的消息

    public ConsumerEnv(String consumerTag, String queueName, boolean autoAck, Consumer consumer) {
        this.consumerTag = consumerTag;
        this.queueName = queueName;
        this.autoAck = autoAck;
        this.consumer = consumer;
    }

    public String getConsumerTag() {
        return consumerTag;
    }

    public void setConsumerTag(String consumerTag) {
        this.consumerTag = consumerTag;
    }

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isAutoAck() {
        return autoAck;
    }

    public void setAutoAck(boolean autoAck) {
        this.autoAck = autoAck;
    }

    public Consumer getConsumer() {
        return consumer;
    }

    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }
}
