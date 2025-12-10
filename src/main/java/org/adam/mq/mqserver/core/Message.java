package org.adam.mq.mqserver.core;

import java.io.Serializable;
import java.util.Arrays;
import java.util.UUID;

/**
 * 这个类表示一个消息（Message），用于在消息队列系统中传递数据。
 * 注意，此处的 Message对象，是需要能够在网络上传输，并且也需要能写入到文件中
 * 此时就需要针对 Message 进行序列化和反序列化操作
 * 此处使用 标准库自带的 序列化机制 来实现
 */
public class Message implements Serializable {
    // 为了让这个类支持序列化，需要定义一个 serialVersionUID,用来记录类的版本信息
    private static final long serialVersionUID = 1L;

    // 这两个属性是消息的基本属性和消息体
    private BasicProperties basicProperties = new BasicProperties();
    private byte[] body;

    // 下面的属性是辅助的属性
    // 一个文件中会存储多条消息，这两个属性用于标识消息在存储文件中的位置 [offsetBeg, offsetEnd)
    // 这两个属性并不需要序列化保存到文件中，此时的消息一旦被写入到文件中，所在的位置就被固定了，并不需要单独存储
    // 这两个属性存在的目的，让内存中的 Message对象，能够知道自己在文件中的位置，从而方便后续的消息读取和管理
    private transient long offsetBeg = 0; // 消息数据的开头距离文件开头的位置偏移(字节)
    private transient long offsetEnd = 0; // 消息数据的结尾距离文件开头的位置偏移(字节)
    // 使用这个属性表示该消息在文件中是否是有效消息(逻辑删除)
    private byte isValid = 0x1; // 标识消息是否有效，0x1表示有效，0x0表示无效(被删除)

    // 创建一个工厂方法，让工厂方法封装创建 Message对象的过程
    // 这个方法中创建的Message对象会自动生成唯一的messageId
    // 这样可以确保每个消息都有一个唯一的标识符，方便后续的消息管理和追踪
    // 万一routingKey 和 basicProperties 里的routingKey不一致时，以参数routingKey为准
    public static Message createMessageWithId(String routingKey,BasicProperties basicProperties, byte[] body) {
        Message message = new Message();
        if (basicProperties != null) {
            message.setBasicProperties(basicProperties);
        }
        // 此处生成的messageId 是以 "M-" 作为前缀
        message.setMessageId("M-" + UUID.randomUUID());
        message.basicProperties.setRoutingKey(routingKey);
        message.setBody(body);
        // 此处是把body 和 basicProperties先设置出来，他们是Message的核心属性
        // 其他属性(比如 offsetBeg, offsetEnd, isValid) 需要在消息存储时(消息持久化)再设置
        // 此处只是在内存中创建一个Message对象
        return message;
    }

    public String getMessageId() {
        return basicProperties.getMessageId();
    }

    public void setMessageId(String messageId) {
        this.basicProperties.setMessageId(messageId);
    }

    public String getRoutingKey() {
        return basicProperties.getRoutingKey();
    }

    public void setRoutingKey(String routingKey) {
        this.basicProperties.setRoutingKey(routingKey);
    }

    public int getDeliveryMode() {
        return basicProperties.getDeliveryMode();
    }

    public void setDeliveryMode(int deliveryMode) {
        this.basicProperties.setDeliveryMode(deliveryMode);
    }

    public BasicProperties getBasicProperties() {
        return basicProperties;
    }

    public void setBasicProperties(BasicProperties basicProperties) {
        this.basicProperties = basicProperties;
    }

    public byte[] getBody() {
        return body;
    }

    public void setBody(byte[] body) {
        this.body = body;
    }

    public long getOffsetBeg() {
        return offsetBeg;
    }

    public void setOffsetBeg(long offsetBeg) {
        this.offsetBeg = offsetBeg;
    }

    public long getOffsetEnd() {
        return offsetEnd;
    }

    public void setOffsetEnd(long offsetEnd) {
        this.offsetEnd = offsetEnd;
    }

    public byte getIsValid() {
        return isValid;
    }

    public void setIsValid(byte isValid) {
        this.isValid = isValid;
    }

    @Override
    public String toString() {
        return "Message{" +
                "basicProperties=" + basicProperties +
                ", body=" + Arrays.toString(body) +
                ", offsetBeg=" + offsetBeg +
                ", offsetEnd=" + offsetEnd +
                ", isValid=" + isValid +
                '}';
    }
}