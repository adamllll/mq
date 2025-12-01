package org.adam.mq.mqserver.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.HashMap;
import java.util.Map;

//MSG => Message Queue
/*
 * 这个类表示一个消息队列（Message Queue），用于存储和管理消息。
 */
public class MSGQueue {
    // 标识队列的身份标识(唯一的）
    private String name;
    // 表示队列是否持久化,true表示持久化，false表示非持久化
    private boolean durable = false;
    // 表示队列是否为排他性的,true表示排他性(表示只能被一个消费者使用)，false表示非排他性(可以被多个消费者使用)
    // 这个 独占 功能 一样暂时不实现
    private boolean exclusive = false;
    // 表示队列在没有消费者时是否自动删除,true表示自动删除，false表示不自动删除
    private boolean autoDelete = false;
    // 用于存储队列的扩展属性，可以包含自定义的键值对
    private Map<String, Object> arguments = new HashMap<>();

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isDurable() {
        return durable;
    }

    public void setDurable(boolean durable) {
        this.durable = durable;
    }

    public boolean isExclusive() {
        return exclusive;
    }

    public void setExclusive(boolean exclusive) {
        this.exclusive = exclusive;
    }

    public boolean isAutoDelete() {
        return autoDelete;
    }

    public void setAutoDelete(boolean autoDelete) {
        this.autoDelete = autoDelete;
    }

    public String getArguments() {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (Exception e) {
            e.printStackTrace();
            return "{}";
        }
    }
    public void setArguments(String argumentsJson) {
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            this.arguments = objectMapper.readValue(argumentsJson, new TypeReference<Map<String, Object>>() {});
        } catch (Exception e) {
            e.printStackTrace();
            this.arguments = new HashMap<>();
        }
    }
}
