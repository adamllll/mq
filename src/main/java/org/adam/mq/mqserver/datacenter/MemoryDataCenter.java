package org.adam.mq.mqserver.datacenter;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 使用这个类来统一管理内存中的数据
 * 该类后续提供的的一些方法，可能会在多线程的环境下被使用，必须考虑线程安全问题
 */
public class MemoryDataCenter {
    private static final Logger log = LoggerFactory.getLogger(MemoryDataCenter.class);
    // key 是 exchangeName, value 是 Exchange 对象
    private ConcurrentHashMap<String, Exchange> exchangeMap = new ConcurrentHashMap<>();
    // key 是 queueName, value 是 MSGQueue 对象
    private ConcurrentHashMap<String, MSGQueue> queueMap = new ConcurrentHashMap<>();
    // key 是 exchangeName, value 是另一个 map， 该 map 的 key 是 routingKey, value 是 Binding 对象
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Binding>> bindingsMap = new ConcurrentHashMap<>();
    // key 是 messageId, value 是 Message 对象
    private ConcurrentHashMap<String, Message> messageMap = new ConcurrentHashMap<>();
    // key 是 queueName, value 是该队列中的消息列表
    private ConcurrentHashMap<String, LinkedList<Message>> queueMessagesMap = new ConcurrentHashMap<>();
    // key 是 queueName, value 是另一个 map，该 map 的 key 是 messageId，value 是 Message 对象
    private ConcurrentHashMap<String, ConcurrentHashMap<String, Message>> queueMessageWaitAckMap = new ConcurrentHashMap<>();

    // 插入交换机对象
    public void insertExchange(Exchange exchange) {
        exchangeMap.put(exchange.getName(), exchange);
        System.out.println("[MemoryDataCenter] 新交换机添加成功，exchangeName=" + exchange.getName());
    }
    // 获取交换机对象
    public Exchange getExchange(String exchangeName) {
        return exchangeMap.get(exchangeName);
    }
    // 删除交换机对象
    public void deleteExchange(String exchangeName) {
        exchangeMap.remove(exchangeName);
        System.out.println("[MemoryDataCenter] 交换机删除成功，exchangeName=" + exchangeName);
    }

    // 插入队列对象
    public void inserQueue(MSGQueue queue) {
        queueMap.put(queue.getName(), queue);
        System.out.println("[MemoryDataCenter] 新队列添加成功，queueName=" + queue.getName());
    }
    // 获取队列对象
    public MSGQueue getQueue(String queueName) {
        return queueMap.get(queueName);
    }
    // 删除队列对象
    public void deleteQueue(String queueName) {
        queueMap.remove(queueName);
        System.out.println("[MemoryDataCenter] 队列删除成功，queueName=" + queueName);
    }

    // 插入绑定对象
    public void insertBinding(Binding binding) throws MqException {
        // 使用ExchangeName查询对应的hashmap是否存在，如果不存在则创建一个新的hashmap
        /*ConcurrentHashMap<String, Binding> bindingMap =  bindingsMap.get(binding.getExchangeName());
        if (bindingMap == null) {
            bindingMap = new ConcurrentHashMap<>();
            bindingsMap.put(binding.getExchangeName(), bindingMap);
        }*/
        // 使用computeIfAbsent方法简化上述逻辑
        ConcurrentHashMap<String, Binding> bindingMap =  bindingsMap.computeIfAbsent(binding.getExchangeName(), k -> new ConcurrentHashMap<>());
        synchronized (bindingMap) { // 对 bindingMap 进行加锁，保证线程安全
            // 再根据 queueName查询，如果已经存在就抛出异常，不存在才能进行插入
            if (binding.getQueueName() != null) {
                throw new MqException("[MemoryDataCenter] 绑定已经存在，不能重复插入 exchangeName=" + binding.getExchangeName() + ", queueName=" + binding.getQueueName());
            }
            bindingMap.put(binding.getQueueName(), binding);
            System.out.println("[MemoryDataCenter] 绑定添加成功，exchangeName=" + binding.getExchangeName() + ", queueName=" + binding.getQueueName());
        }
    }
    // 获取绑定
    // 1.根据exchangeName和queueName确定唯一个一的binding对象
    public Binding getBinding(String exchangeName, String queueName) {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(exchangeName);
        if (bindingMap != null) {
            return bindingMap.get(queueName);
        }
        return null;
    }
    // 2.根据exchangeName获取该交换机下的所有binding对象
    public ConcurrentHashMap<String, Binding> getBindings(String exchangeName) {
        return bindingsMap.get(exchangeName);
    }
    // 删除绑定对象
    public void deleteBinding(Binding binding) throws MqException {
        ConcurrentHashMap<String, Binding> bindingMap = bindingsMap.get(binding.getExchangeName());
        if (binding == null) {
            // 如果bindingMap不存在，说明该交换机下没有任何绑定
            throw new MqException("[MemoryDataCenter] 绑定不存在! exchangeName=" + binding.getExchangeName() + ", queueName=" + binding.getQueueName());
        }
        bindingMap.remove(binding.getQueueName());
        System.out.println("[MemoryDataCenter] 绑定删除成功，exchangeName=" + binding.getExchangeName() + ", queueName=" + binding.getQueueName());
    }

    // 添加消息
    public void addMessage(Message message) {
        messageMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 新消息添加成功，messageId=" + message.getMessageId());
    }
    // 根据id查询消息
    public Message getMessage(String messageId) {
        return messageMap.get(messageId);
    }
    // 根据id删除消息
    public void deleteMessage(String messageId) {
        messageMap.remove(messageId);
        System.out.println("[MemoryDataCenter] 删除消息成功，messageId=" + messageId);
    }
    // 发送消息到指定队列
    public void sendMessage(MSGQueue queue, Message message) {
        // 把消息放到对应的数据结构中
        // 先根据队列的名字，找到对应的消息链表
        // 如果不存在，则创建一个新的链表
        LinkedList<Message> messages = queueMessagesMap.computeIfAbsent(queue.getName(), k -> new LinkedList<>());
        synchronized (messages) {
            messages.addLast(message);
        }
        // 在这里把该消息也往消息中心插入一下,假设如果message已经存在，重复插入就会覆盖
        // 原因在于相同的messageId对应的message的内容是一样的(服务器代码不会对message的内容进行修改 basicProperties和body不会被修改)
        addMessage(message);
        System.out.println("[MemoryDataCenter] 消息发送到队列成功，queueName=" + queue.getName() + ", messageId=" + message.getMessageId());
    }
    // 从队列中取消息
    public Message pollMessage(String queueName) {
        // 根据队列名，查找一下对应的消息链表
        LinkedList<Message> messages = queueMessagesMap.get(queueName);
        if (messages == null) {
            // 说明该队列没有任何消息，直接返回null
            return null;
        }
        synchronized (messages) {
            // 如果没有找到，说明该队列没有任何消息，直接返回null
            if (messages.size() == 0) {
                return null;
            }
            // 链表中有元素，进行头删
            Message currentMessage = messages.remove(0);
            System.out.println("[MemoryDataCenter] 从队列中获取消息成功，queueName=" + queueName + ", messageId=" + currentMessage.getMessageId());
            return currentMessage;
        }
    }
    // 获取指定队列中的消息个数
    public int getMessageCount(String queueName) {
        LinkedList<Message> messages = queueMessagesMap.get(queueName);
        if (messages == null) {
            // 说明该队列没有任何消息
            return 0;
        }
        synchronized (messages) {
            return messages.size();
        }
    }

    // 添加未确认的消息
    public void addMessageWaitAck(String queueName, Message message) {
        // 先根据队列名，找到对应的未确认消息的map
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.computeIfAbsent(queueName, k -> new ConcurrentHashMap<>());
        messageHashMap.put(message.getMessageId(), message);
        System.out.println("[MemoryDataCenter] 未确认消息添加成功，queueName=" + queueName + ", messageId=" + message.getMessageId());
    }
    // 删除未确认的消息
    public void removeMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if (messageHashMap != null) {
            messageHashMap.remove(messageId);
            System.out.println("[MemoryDataCenter] 未确认消息删除成功，queueName=" + queueName + ", messageId=" + messageId);
        }
    }
    // 获取指定的未确认消息
    public Message getMessageWaitAck(String queueName, String messageId) {
        ConcurrentHashMap<String, Message> messageHashMap = queueMessageWaitAckMap.get(queueName);
        if (messageHashMap != null) {
            return messageHashMap.get(messageId);
        }
        return null;
    }
}
