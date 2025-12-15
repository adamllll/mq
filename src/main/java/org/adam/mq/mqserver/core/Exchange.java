package org.adam.mq.mqserver.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

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
   // 为了把这个 augments 存到数据库中，就需要把这个map转成json格式的字符串
    private Map<String, Object> arguments = new HashMap<>();

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

    // 这里的get和set用于和数据库交互
    public String getArguments() {
        // 把 augments 从 Map 转成 String(JSON)
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            return objectMapper.writeValueAsString(arguments);
        } catch (Exception e) {
            e.printStackTrace();
            // 如果代码真异常了，就返回一个空的json对象
            return "{}";
        }
    }

    // 从数据库度数据之后，构造 Exchange对象，会自动调用到
    public void setArguments(String argumentsJson) {
        // 把参数中的 argumentsJson 按照 JSON格式解析转成上述的 Map对象
        ObjectMapper objectMapper = new ObjectMapper();
        try {
            // 注意这里的 TypeReference 用法, 用于告诉 ObjectMapper 目标类型是 Map<String, Object>
           this.arguments =  objectMapper.readValue(argumentsJson, new TypeReference<HashMap<String, Object>>() {});
        } catch (Exception e) {
            e.printStackTrace();
            // 如果代码真异常了，就把 augments 设置成一个空的map
            this.arguments = new HashMap<>();
        }
    }

    // 针对argument再提供一组 getter setter，用来更方便的去获取/设置这里的键值对
    // 这一组方法不会被数据库操作调用到，主要在Java内部的代码使用（测试）
    public Object getArguments(String key) {
        return arguments.get(key);
    }
    public void setArguments(String key, Object value) {
        arguments.put(key, value);
    }
    // 再添加一个新的方法, 用于一次性设置整个arguments map
    public void setArguments(Map<String, Object> arguments) {
        this.arguments = arguments;
    }

}
