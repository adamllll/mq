package org.adam.mq.common;

import org.adam.mq.mqserver.core.ExchangeType;

import java.io.Serializable;
import java.util.Map;

public class ExchangeDeclareArguments extends BasicArguments implements Serializable {
    private String exchangeName; // 交换机名称
    private ExchangeType exchangeType; // 交换机类型
    private boolean durable; // 是否持久化
    private boolean autoDelete; // 是否自动删除
    private Map<String, Object> arguments; // 其他参数

    public Map<String, Object> getArguments() {
        return arguments;
    }

    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

    public String getExchangeName() {
        return exchangeName;
    }

    public void setExchangeName(String exchangeName) {
        this.exchangeName = exchangeName;
    }

    public ExchangeType getExchangeType() {
        return exchangeType;
    }

    public void setExchangeType(ExchangeType exchangeType) {
        this.exchangeType = exchangeType;
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
}
