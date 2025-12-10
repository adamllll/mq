package org.adam.mq.mqserver.core;

import java.io.Serializable;

/**
 * 这个类表示消息的基本属性（Basic Properties），用于存储消息的元数据。
 */
public class BasicProperties implements Serializable { // 在执行testSendMessage发现没有实现Serializable接口会报错，这是由于Message类中包含了BasicProperties属性，当使用 Java 标准序列化机制时,如果一个类实现了 Serializable 接口,那么它的所有非 transient 成员变量也必须是可序列化的。
    // 消息的唯一身份标识符,为了保证id的唯一性，可以使用UUID来生成。
    // UUID是一种标准的用于标识信息的(一种算法)128位长的数字，一般用32个字符的十六进制数表示，通常以连字符分隔成五段，
    // 形式如 "550e8400-e29b-41d4-a716-446655440000"。
    private String messageId;
    // 消息的路由键，用于指定消息的路由规则
    // 这个路由键会被用来和交换机的类型以及绑定的路由规则进行匹配，从而决定消息应该被发送到哪些队列。
    // 比如：当前交换机类型是DIRECT，此时routingKey 就表示要转发的队列名称
    // 如果交换机类型是TOPIC，那么routingKey 就要和 bindingkey 进行模糊匹配，从而决定消息要发送到哪些队列
    // 如果交换机类型是FANOUT，那么routingKey 通常会被忽略，因为FANOUT交换机会将消息广播到所有绑定的队列
    private String routingKey;

    private int deliveryMode = 1; // 传递模式，例如 1 表示非持久化，2 表示持久化

    // 针对RabbitMQ来说，Basicproperties 还有很多其他属性，但为了简化实现，这里只实现了上述几个常用属性。

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public String getRoutingKey() {
        return routingKey;
    }

    public void setRoutingKey(String routingKey) {
        this.routingKey = routingKey;
    }

    public int getDeliveryMode() {
        return deliveryMode;
    }

    public void setDeliveryMode(int deliveryMode) {
        this.deliveryMode = deliveryMode;
    }

    @Override
    public String toString() {
        return "BasicProperties{" +
                "messageId='" + messageId + '\'' +
                ", routingKey='" + routingKey + '\'' +
                ", deliveryMode=" + deliveryMode +
                '}';
    }
}
