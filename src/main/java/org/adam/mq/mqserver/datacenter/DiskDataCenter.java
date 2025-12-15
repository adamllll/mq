package org.adam.mq.mqserver.datacenter;

import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.Binding;
import org.adam.mq.mqserver.core.Exchange;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

/**
 * 使用这个类来管理所有硬盘上的数据
 * 1。 数据库：交换机，绑定，队列
 * 2. 数据文件：消息
 * 上层逻辑如果需要操作硬盘，统一通过这个类来使用(上层代码不关心当前数据存储在数据库还是文件中)
 */
public class DiskDataCenter {
    // 这个实例用来管理数据库中的数据
    private DataBaseManager dataBaseManager = new DataBaseManager();
    // 这个实例用来管理硬盘上的消息文件
    private MessageFileManager messageFileManager = new MessageFileManager();

    public void init() {
        // 针对数据库和消息文件都进行初始化操作
        dataBaseManager.init();
        // messageFileManager.init()当前是空方法，后续扩展就在此进行初始化
        messageFileManager.init();
    }

    // 封装交换机操作
    public void insertExchange(Exchange exchange) {
        dataBaseManager.insertExchange(exchange);
    }
    public void deleteExchange(String exchangeName) {
        dataBaseManager.deleteExchange(exchangeName);
    }
    public List<Exchange> selectAllExchanges() {
        return dataBaseManager.selectAllExchanges();
    }

    // 封装队列操作
    public void insertQueue(MSGQueue queue) throws IOException {
        dataBaseManager.insertQueue(queue);
        // 创建队列的同时，不仅仅把队列对象写到数据库中，还需要创建出对应的目录和文件
        messageFileManager.createQueueFiles(queue.getName());
    }
    public void deleteQueue(String queueName) throws IOException {
        dataBaseManager.deleteQueue(queueName);
        // 删除队列的同时，还需要把对应的消息文件给删除掉
        messageFileManager.destroyQueueFiles(queueName);
    }
    public List<MSGQueue> selectAllQueues() {
        return dataBaseManager.selectAllQueues();
    }

    // 封装绑定操作
    public void insertBinding(Binding binding) {
        dataBaseManager.insertBinding(binding);
    }
    public void deleteBinding(Binding binding) {
        dataBaseManager.deleteBinding(binding);
    }
    public List<Binding> selectAllBindings() {
        return dataBaseManager.selectAllBindings();
    }

    // 封装消息操作
    public void sendMessage(MSGQueue queue, Message message) throws IOException, MqException {
        messageFileManager.sendMessage(queue, message);
    }
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException, MqException {
        messageFileManager.deleteMessage(queue, message);
        // 判断是否需要gc
        if (messageFileManager.checkGC(queue.getName())) {
            messageFileManager.gc(queue);
        }
    }
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        return messageFileManager.loadAllMessageFromQueue(queueName);
    }
}
