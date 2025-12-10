package org.adam.mq.mqserver.datacenter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.lang.annotation.Target;
import java.util.LinkedList;
import java.util.Scanner;

import org.adam.mq.common.BinaryTool;
import org.adam.mq.common.MqException;
import org.adam.mq.mqserver.core.MSGQueue;
import org.adam.mq.mqserver.core.Message;

public class MessageFileManager {
    // 定义一个内部类来表示队列的统计信息
    // 优先考虑使用 static，静态内部类不依赖于外部类的实例，可以直接通过类名访问
    static public class Stat {
        // 直接定义为public，方便外部访问
        // 对于这样的简单类，就直接使用成员变量，不使用getter/setter方法
        public int totalCount; // 总消息数
        public int validCount; // 有效消息数
    }

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

    // 这个方法用来获取该队列的消息统计文件路径
    private String getQueueStatPath(String queueName) {
        return getQueueDir(queueName) + "/queue_stat.txt";
    }

    private Stat readStat(String queueName) {
        // 由于当前的消息统计文件是文本文件，可以直接使用Scanner来读取文件
        Stat stat = new Stat();
        // 修改：使用 try-with-resources 自动关闭流，防止资源泄漏
        try (InputStream inputStream = new FileInputStream(getQueueStatPath(queueName));
             Scanner scanner = new Scanner(inputStream)) {
            stat.totalCount = scanner.nextInt();
            stat.validCount = scanner.nextInt();
            return  stat;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private void writeStat(String queueName, Stat stat) {
        // 使用 PrintWriter 来写入文本文件
        // OutputStream打开文件，默认情况下会把原有内容覆盖掉，此时就相当于重写
        // 修改：使用 try-with-resources 自动关闭流，防止资源泄漏
        try (OutputStream outputStream = new FileOutputStream(getQueueStatPath(queueName));
             PrintWriter printWriter = new PrintWriter(outputStream)) {
            printWriter.write(stat.totalCount + "\t" + stat.validCount);
            printWriter.flush();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    // 创建队列对应的文件和目录
    public void createQueueFiles(String queueName) throws IOException {
        // 1. 先创建队列对应的消息目录
        File baseDir = new File(getQueueDir(queueName));
        if (!baseDir.exists()) {
            // 不存在就创建目录
           boolean ok =  baseDir.mkdirs();
           if (!ok) {
               throw new IOException("[MessageFileManager] 创建目录失败: " + baseDir.getAbsolutePath());
           }
        }
        // 2. 创建消息数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
           boolean ok = queueDataFile.createNewFile();
              if (!ok) {
                throw new IOException("[MessageFileManager] 创建文件失败: " + queueDataFile.getAbsolutePath());
              }
        }
        // 3. 创建消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            boolean ok = queueStatFile.createNewFile();
            if (!ok) {
                throw new IOException("[MessageFileManager]创建文件失败: " + queueStatFile.getAbsolutePath());
            }
        }
        // 4. 初始化统计信息,设定初始值,0\t0
        Stat stat = new Stat();
        stat.totalCount = 0;
        stat.validCount = 0;
        writeStat(queueName, stat);
    }

    // 删除队列对应的文件和目录
    // 队列也是可以删除的，当队列删除之后，对应的消息文件和目录也应该删除掉
    public void destroyQueueFiles(String queueName) throws IOException {
        // 1. 删除消息数据文件
        File queueDataFile = new File(getQueueDataPath(queueName));
        boolean ok1 = queueDataFile.delete();
        // 2. 删除消息统计文件
        File queueStatFile = new File(getQueueStatPath(queueName));
        boolean ok2 = queueStatFile.delete();
        // 3. 删除队列目录
        File baseDir = new File(getQueueDir(queueName));
        boolean ok3 = baseDir.delete();
        if (!ok1 || !ok2 || !ok3) {
            // 有任意一个删除失败，就打印日志
            throw new IOException("[MessageFileManager] 删除队列文件失败: " + queueName);
        }
    }

    // 检查队列的目录和文件是否存在
    // 比如后续有生产者给 broker server 生产消息了，这个消息就可能需要记录到文件上(取决于消息是否需要持久化)
    public boolean checkFilesExists(String queueName) {
        // 文件存在隐含目录存在
        // 如果 queue_data.txt 和 queue_stat.txt 两个文件都存在,那么它们所在的目录必然存在
        // 文件不可能存在于一个不存在的目录中
        // 只检查消息数据文件和消息统计文件是否存在
        File queueDataFile = new File(getQueueDataPath(queueName));
        if (!queueDataFile.exists()) {
            return false;
        }
        File queueStatFile = new File(getQueueStatPath(queueName));
        if (!queueStatFile.exists()) {
            return false;
        }
        return true;
    }
    // 这个方法用来把一个新的消息放入到指定队列的消息文件中
    // queue: 消息队列对象(要把消息写入的队列), message: 要存储的消息对象(要写的消息)
    public void sendMessage(MSGQueue queue, Message message) throws MqException,IOException {
        // 1.检查一下当前要写入的队列对应的文件是否存在
        if (!checkFilesExists(queue.getName())) {
            throw new MqException("[MessageFileManager] 消息队列对应的文件不存在！queueName= " + queue.getName());
        }

        // 2.把 Message 对象进行序列化，转成二进制的字节数组
        byte[] messageBinary = BinaryTool.toBytes(message);

        // 由于可能有多个线程同时往同一个队列写消息，所以这里需要进行同步处理
        synchronized (queue) {
            // 3.先获取当前队列数据文件的长度，用来计算新消息的写入位置(计算出该 Message的offsetBeg和offsetEnd)
            // 把新的 Message数据，写入到队列数据文件的末尾，此时Message对象的offsetBeg就是文件当前的长度 + 4
            // offsetEnd就是当前文件长度 + 4 + messageBinary.length(message自身长度)
            File queueDataFile = new File(getQueueDataPath(queue.getName()));
            // 通过这个方法就能queueDataFile.length()就能获取到文件长度，单位字节
            message.setOffsetBeg(queueDataFile.length() + 4); // +4是因为还要存储消息长度的4个字节
            message.setOffsetEnd(queueDataFile.length() + 4 + messageBinary.length);

            // 4.写入消息到文件中,追加写入到文件末尾
            try (OutputStream outputStream = new FileOutputStream(queueDataFile, true)) {
                // 先写入消息的长度(4个字节)
                // 在流对象中写入一个int类型的数据，需要把int转换成4个字节的byte数组(把int的四个字节分别取出来，一个一个字节的写)
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    dataOutputStream.writeInt(messageBinary.length);
                    // 再写入消息的二进制数据(消息本体)
                    dataOutputStream.write(messageBinary);
                }
            }

            // 5.更新消息统计文件
            Stat stat = readStat(queue.getName());
            stat.totalCount += 1;
            stat.validCount += 1;
            writeStat(queue.getName(), stat);
        }
    }
    // 这个方法用来删除指定队列中的指定消息,也就是把硬盘上存储这个数据的 isValid 标记改为 0
    // 此处参数中的 Message 对象，必须包含 offsetBeg 和 offsetEnd 两个属性
    public void deleteMessage(MSGQueue queue, Message message) throws IOException, ClassNotFoundException {
        // 1.先把文件中的这一段数据，都取出来，返回原 Message对象
        // 2. 把 isValid 标记改为 0
        // 3. 把修改后的 Message 对象重新写回到文件的原位置
        synchronized (queue) { // 同步，防止多个线程同时修改同一个文件
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(getQueueDataPath(queue.getName()), "rw")) { // 修复1错误的文件路径getQueueDataPath
                // 1. 先从文件中读取对应的 Message 对象
                byte[] bufferSrc = new byte[(int) (message.getOffsetEnd() - message.getOffsetBeg())];
                randomAccessFile.seek(message.getOffsetBeg());
                randomAccessFile.read(bufferSrc); // 读取原始数据到bufferSrc中
                // 2. 把当前读出来的二进制数据转换为 Message 对象
                Message diskMessage = (Message) BinaryTool.fromBytes(bufferSrc);
                // 3. 把 isValid 标记改为 0
                diskMessage.setIsValid((byte) 0x0);
                //此处不需要给参数的这个 message的isValid设为0，因为这个参数代表的是内存中管理的Message对象
                // 而这个对象的isValid并不影响文件中的数据，文件中的数据才是最终的存储结果(马上要被从内存中销毁了)
                // 4. 把修改后的 Message 对象重新写回到文件的原位置
                byte[] bufferDest = BinaryTool.toBytes(diskMessage); // 把修改后的 Message 对象转换为二进制数据
                // 虽然上面的操作已经seek过了，但是上面的seek操作之后进行了读操作，读操作会改变文件指针的位置
                // 所以这里还是需要重新seek一下
                randomAccessFile.seek(message.getOffsetBeg()); // 定位到原来消息的起始位置
                //但实际上，由于消息的长度字段已经在 offsetBeg 之前（偏移量 -4 的位置），这行代码完全是多余的，应该直接写入消息体：
                randomAccessFile.write(bufferDest); // 直接写入修改后的消息对象
//                randomAccessFile.writeInt(bufferDest.length); // 先写入消息长度修复2直接写入消息体，不需要单独改变长度
                // 通过上述的操作，对于文件来说，只是有一个字节发生了改变
            }
            // 5.更新消息统计文件,有效消息数减1
            Stat stat = readStat(queue.getName());
            if (stat.validCount > 0) {
                stat.validCount -= 1;
            }
            writeStat(queue.getName(), stat);
        }
    }

    // 使用这个方法从指定队列的消息文件中，加载出所有的消息对象
    // 这个方法主要用于 broker server 启动的时候，把硬盘上的消息加载到内存中
    // LinkedList 是一个链表数据结构，适合频繁的插入和删除操作(头删尾插)
    // 这个方法的参数只是一个 queueName字符串，而不是 MSGQueue 对象,这是因为这个方法不需要加锁，只使用queueName就够了
    // 由于该方法是在程序启动的时候调用，此时服务器还不能能处理其他请求，所以不需要考虑并发问题(不用加锁)
    public LinkedList<Message> loadAllMessageFromQueue(String queueName) throws IOException, ClassNotFoundException {
        LinkedList<Message> messages = new LinkedList<>();
        try (InputStream inputStream = new FileInputStream(getQueueDataPath(queueName))) {
            try (DataInputStream dataInputStream = new DataInputStream(inputStream)) {
                // 使用这个变量记录当前文件光标的位置
                long currentOffset = 0;
                // 一个文件中包含若干个消息，此处需要循环读取每个消息
                while (true) {
                    // 1. 先读取消息的长度(4个字节)，如果读不到数据，说明已经到文件末尾了(EOF)，跳出循环
                    // readInt() 方法在到达文件末尾时会抛出 EOFException 异常,这一点和之前的很对流对象不一样
                    // 所以这里使用捕获异常的方式来判断是否到达文件末尾
                   int messageSize = dataInputStream.readInt();
                   // 2. 按照这个长度，读取对应的消息二进制数据
                   byte[] buffer = new byte[messageSize];
                   int actualSize = dataInputStream.read(buffer);
                     if (actualSize != messageSize) { 
                        // 读取到的字节数与预期不符,说明文件可能损坏，格式错乱了
                        throw new IOException("[MessageFileManager] 读取消息数据异常，读取到的字节数与预期不符 quneueName=" + queueName);
                     }
                     // 3. 把读取到的二进制数据，转换为 Message 对象
                     Message message = (Message) BinaryTool.fromBytes(buffer);
                     // 4. 判定一下这个消息对象是否是有效的
                     if (message.getIsValid() != (byte) 0x1) {
                            // 无效消息，跳过
                            // 虽然是无效的消息，但是文件光标位置还是要更新
                            currentOffset += (4 + messageSize); // 更新文件光标位置
                            continue;
                        }
                    // 5. 有效数据，就把这个消息对象，添加到返回的列表中
                    // 加入之前还需要设置一下消息的 offsetBeg 和 offsetEnd 属性
                    // 进行计算offsetBeg 和 offsetEnd的位置，还需要知道当前文件光标的位置，但是由于当前使用的是 DataInputStream不方便直接获取到文件光标的位置
                    // 所以我们手动计算一下文件光标的位置
                    // 文件光标的位置 = 已经读取的消息数据的总长度 + 4(消息长度字段)
                    // 已经读取的消息数据的总长度 = 当前消息的长度 + 前面所有消息的长度之和
                    message.setOffsetBeg(currentOffset + 4);
                    message.setOffsetEnd(currentOffset + 4 + messageSize);
                    currentOffset += (4 + messageSize); // 更新文件光标位置
                    messages.add(message);
                }
            }catch (EOFException e) {
                // 捕获到 EOFException 异常，说明可能是到达了文件末尾
                // 这个catch并非异常处理逻辑，而是用来跳出上面的while(true)循环，文件读到末尾会被readInt抛出EOFException
                // 直接返回已经读取到的消息列表
                System.out.println("[MessageFileManager] 从文件加载消息完成，queueName=" + queueName + " , 共加载到 " + messages.size() + " 条消息");
            }
        }
        return messages;

    }

    // 检查当前是否要针对该队列的消息文件数据进行GC
    public boolean checkGC(String queueName) {
        // 判定是否要GC是根据总消息数和有效消息数的比例来决定的
        Stat stat = readStat(queueName);
        if (stat.totalCount > 2000 && (double)stat.validCount / (double)stat.totalCount < 0.5) {
            return true;
        }
        return false;
    }

    // 对指定队列的消息文件进行GC
    private String getQueueDataNewPath(String queueName) {
        return getQueueDir(queueName) + "/queue_data_new.txt";
    }

    // 通过这个方法真正执行消息数据文件的垃圾回收操作
    // 使用复制算法执行GC
    // 把有效的消息复制到一个新的文件中，然后把旧文件删除，最后把新文件重命名为旧文件名
    // 创建一个新文件queue_data_new.txt，把有效消息写入到这个新文件中,删除旧文件queue_data.txt,把新文件重命名为旧文件名
    // 同时要记得更新消息统计文件
    public void gc(MSGQueue queue) throws MqException, IOException, ClassNotFoundException {
        // 进行gc的时候，是针对消息文件的大洗牌，在这个过程中，不能有其他线程对该队列进行读写操作
        synchronized (queue) {
            // 由于gc操作比较耗时，此处统计执行消耗的时间
            long startTime = System.currentTimeMillis();
            // 1. 创建一个新的文件
            File queueDataNewFile = new File(getQueueDataNewPath(queue.getName()));
            if (queueDataNewFile.exists()) {
                // 正常情况下这个文件是不存在的，如果存在就说明上次GC没有完成
                throw new MqException("[MessageFileManager] 发现上次GC未完成，无法进行新的GC操作，queueName=" + queue.getName());
            }
            boolean ok = queueDataNewFile.createNewFile();
            if (!ok) {
                throw new MqException("[MessageFileManager] 创建GC临时文件失败，queueName=" + queue.getName() + "queueDataNewFile=" + queueDataNewFile.getAbsolutePath());
            }
            // 2. 把有效消息复制到新文件中,从旧文件中读取出有效消息(这个逻辑直接调用上述的方法，不需要重新写)
            LinkedList<Message> messages = loadAllMessageFromQueue(queue.getName());

            // 3. 把有效消息写入到新文件中
            try (OutputStream outputStream = new FileOutputStream(queueDataNewFile, true)) {
                try (DataOutputStream dataOutputStream = new DataOutputStream(outputStream)) {
                    for (Message message : messages) {
                        // 把 Message 对象转换为二进制数据
                        byte[] buffer = BinaryTool.toBytes(message);
                        // 写入消息长度
                        dataOutputStream.writeInt(buffer.length);
                        // 写入消息数据
                        dataOutputStream.write(buffer);
                    }
                }
            }

            // 4. 删除旧文件,并且把新文件重命名为旧文件名
            File queueDataOldFile = new File(getQueueDataPath(queue.getName()));
            ok = queueDataOldFile.delete();
            if (!ok) {
                throw new MqException("[MessageFileManager] 删除旧消息文件失败，queueName=" + queue.getName() + " , filePath=" + queueDataOldFile.getAbsolutePath());
            }
            // 重命名新文件为旧文件名,queue_data_new.txt -> queue_data.txt
            ok = queueDataOldFile.renameTo(queueDataNewFile);
            if (!ok) {
                throw new MqException("[MessageFileManager] 重命名新消息文件失败，queueName=" + queue.getName() + " , oldFilePath=" + queueDataOldFile.getAbsolutePath() + " , newFilePath=" + queueDataNewFile.getAbsolutePath());
            }

            // 5. 更新消息统计文件
            Stat stat =readStat(queue.getName());
            stat.totalCount = messages.size();
            stat.validCount = messages.size();
            writeStat(queue.getName(), stat);

            long endTime = System.currentTimeMillis();
            System.out.println("[MessageFileManager] 对队列 " + queue.getName() + " 进行消息文件GC完成，耗时 " + (endTime - startTime) + " 毫秒");
        }
    }
}
