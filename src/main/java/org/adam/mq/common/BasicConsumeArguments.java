package org.adam.mq.common;

import java.io.Serializable;

public class BasicConsumeArguments extends BasicArguments implements Serializable {
    private String consumerTag; // 消费者标签
    private String queueName; // 队列名称
    private boolean autoAck; // 是否自动确认

    // 这个类中对应的 basicConsume 方法的参数含有一个回调函数(无法通过网络传输)
    // 在服务器 broker server 针对消息处理的回调是统一的(把消息返回给客户端)
    // 客户端收到消息之后，再调用用户自定义的回调函数进行处理
    // 那么客户端就不需要把自身的回调函数传输到服务器端了 所以不需要在这里定义回调函数参数

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
}
