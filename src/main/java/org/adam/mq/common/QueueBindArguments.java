package org.adam.mq.common;

import java.io.Serializable;

public class QueueBindArguments extends BasicArguments implements Serializable {
    private String queueName; // 队列名称
    private String exchangeName; // 交换机名称
    private String bindingKey; // 绑定键

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public String getBindingKey() {
        return bindingKey;
    }

    public void setBindingKey(String bindingKey) {
        this.bindingKey = bindingKey;
    }
}

