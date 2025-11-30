package org.adam.mq.mqserver.core;

import java.util.HashMap;
import java.util.Map;

/**
 * 这个类表示一个交换机（Exchange），用于在消息队列系统中路由消息到相应的队列。
 */
public class Exchange {
    // 此处使用name来作为交换机的身份标识（唯一的）
    private String name;
    // 交换机类型, 例如 direct, topic, fanout
    private ExchangeType type = ExchangeType.DIRECT;
    // 交换机是否持久化 (服务器重启后是否依然存在),true表示持久化，false表示非持久化
    private boolean durable = true;
    // 交换机在没有绑定队列时是否自动删除，true表示自动删除，false表示不自动删除
    // 这个属性暂时没有被使用到（后续的代码并没有真的实现）
    private boolean autoDelete = false;
    // 交换机的附加属性(参数选项)，可以存储一些自定义的信息(一样没有被使用到，后续的代码并没有真的实现)
    private Map<String, Object> augments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public ExchangeType getType() {
        return type;
    }

    public void setType(ExchangeType type) {
        this.type = type;
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

    public Map<String, Object> getAugments() {
        return augments;
    }

    public void setAugments(Map<String, Object> augments) {
        this.augments = augments;
    }
}
