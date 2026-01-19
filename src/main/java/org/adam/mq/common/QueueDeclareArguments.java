package org.adam.mq.common;

import java.io.Serializable;
import java.util.Map;

public class QueueDeclareArguments extends BasicArguments implements Serializable {
    private String queueName; // 队列名称
    private boolean exclusive; // 是否排他
    private boolean durable; // 是否持久化
    private boolean autoDelete; // 是否自动删除
    private Map<String, Object> arguments; // 其他参数

    public String getQueueName() {
        return queueName;
    }

    public void setQueueName(String queueName) {
        this.queueName = queueName;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }
}
