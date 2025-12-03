package org.adam.mq.mqserver.datacenter;

public class MessageFileManager {
    // 约定消息文件所在的目录和文件名
    // 这个方法用来获取指定队列对应的消息文件所在路径
    private String getQueueDir(String queueName) {
        return "./data/" + queueName;
    }
    // 这个方法用来获取该队列的消息数据文件路径
    // 二进制文件使用txt后缀只是为了方便查看，实际存储可以使用其他格式
    private String getQueueDataPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data.txt";
    }
}
