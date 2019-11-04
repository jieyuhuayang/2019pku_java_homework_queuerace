package io.test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/***
 * 实现一个基于内存的单线程 队列引擎 
 * 生产者发送指定量的消息给不同的队列 
 * 消费者随机从队列中读取一串连续消息进行校验
 * 校验过程需要保证消息连续
 * 完成4处代码填充，每处一行代码
 */
public class Memory {

	public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

	public static void main(String[] args) throws Exception {
		// 队列的数量
		int queueAmount = 100 * 1000;
		// 生产者总的消息发送量
		int messageSendAmount = 20 * 1000 * 1000;
		// 消费者校验的次数
		int messageCheckTimes = 1 * 1000 * 1000  ;

		// 各个队列的消息存储区域
		Map<String, List<byte[]>> queueMap = new HashMap<String, List<byte[]>>();

		// 生产者
		Producer producer = new Producer(queueMap);
		// 消费者
		Consumer consumer = new Consumer(queueMap);
		// 每次发送和校验选择随机队列
		Random random = new Random();

		// 队列名字集合
		List<String> queueNameList = new ArrayList<>();

		// 每个队列对应消息的数量
		Map<String, Integer> queueSizeMap = new HashMap<>();

		long startTime = System.currentTimeMillis();

		// 生成队列
		for (int i = 0; i < queueAmount; i++) {
			String queueName = "Queue-" + i;
			queueNameList.add(queueName);
			queueSizeMap.put(queueName, 0);
		}

		// 发送指定数量的消息给随机任意的队列
		int sendProcess = 0;
		for (int i = 0; i < messageSendAmount; i++) {
			int indexOfQueueName = random.nextInt(queueAmount);

			// **************** 1 获得队列名称 ******************
			String queueName = queueNameList.get(indexOfQueueName);

			int messageCount = queueSizeMap.get(queueName);
			byte[] data = (queueName + " " + messageCount).getBytes();
			producer.put(queueName, data);

			// **************** 2 修改队列当前的message数量 ******************
			queueSizeMap.put(queueName,messageCount+1);

			if (i % (messageSendAmount / 10) == 0) {
				System.out.printf("send process is %d%% ... \n", sendProcess * 10);
				sendProcess++;
			}
		}
		
		// 校验数据是否准确
		int checkProcess = 0;
		for (int i = 0; i < messageCheckTimes; i++) {
			int indexOfQueueName = random.nextInt(queueAmount);
			String queueName = queueNameList.get(indexOfQueueName);
			// 队列消息数量取随机数-10
			int checkStartIndex = random.nextInt(queueSizeMap.get(queueName)) - 10;
			if (checkStartIndex < 0) {
				checkStartIndex = 0;
			}
			Collection<byte[]> msgs = consumer.get(queueName, checkStartIndex, 10);
			for (byte[] msg : msgs) {
				// 当前队列消息数量
				int msgId = Integer.valueOf(new String(msg).split(" ")[1]);
				if (msgId != checkStartIndex++) {
					System.out.printf("check error: get [%d] expect [%d]", msgId, checkStartIndex - 1);
				}
			}
			if (i % (messageCheckTimes / 10) == 0) {
				System.out.printf("check process is %d%% ... \n", checkProcess * 10);
				checkProcess++;
			}
		}
		long endTime = System.currentTimeMillis();
		//统计运行时间
		System.out.printf("cost time : [%.2f] s", (endTime - startTime + 0.1) / 1000);
	}
}

class Producer {
	Map<String, List<byte[]>> queueMap;

	public Producer(Map<String, List<byte[]>> queueMap) {
		this.queueMap = queueMap;
	}

	public void put(String queueName, byte[] message) {
		if (!queueMap.containsKey(queueName)) {
			queueMap.put(queueName, new ArrayList<byte[]>());
		}
		// **************** 3 将message放入指定的队列中 ******************
		queueMap.get(queueName).add(message);

	}
}

class Consumer {
	Map<String, List<byte[]>> queueMap;

	public Consumer(Map<String, List<byte[]>> queueMap) {
		this.queueMap = queueMap;
	}

	public Collection<byte[]> get(String queueName, long offset, long num) {
		if (!queueMap.containsKey(queueName)) {
			return Memory.EMPTY;
		}
		// **************** 4 从指定队列中提取消息list集合：msgs ******************
		List<byte[]> msgs = queueMap.get(queueName);

		return msgs.subList((int) offset, offset + num > msgs.size() ? msgs.size() : (int) (offset + num));
	}
}
