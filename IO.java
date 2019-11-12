import java.io.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/***
 * 实现一个基于IO的单线程队列引擎 
 * 完成两处读写文件的操作，put和get，文件存放在data目录下
 * 目前只需要支持少量队列数量
 */
public class IO {

	public static Collection<byte[]> EMPTY = new ArrayList<byte[]>();

	public static void main(String[] args) throws Exception {
		// 队列的数量
		int queueAmount = 100;
		// 生产者总的消息发送量
		int messageSendAmount = 20 * 1000;
		// 消费者校验的次数
		int messageCheckTimes = 1 * 1000;

		// 生产者
		Producer producer = new Producer();
		// 消费者
		Consumer consumer = new Consumer();
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

			String queueName = queueNameList.get(indexOfQueueName);

			int messageCount = queueSizeMap.get(queueName);
			byte[] data = (queueName + " " + messageCount).getBytes();
			producer.put(queueName, data);

			queueSizeMap.put(queueName, messageCount + 1);

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
			int checkStartIndex = random.nextInt(queueSizeMap.get(queueName)) - 10;
			if (checkStartIndex < 0) {
				checkStartIndex = 0;
			}
			Collection<byte[]> msgs = consumer.get(queueName, checkStartIndex, 10);
			for (byte[] msg : msgs) {
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

	public void put(String queueName, byte[] message) throws IOException {
		File dir = new File("data");
		if(!dir.exists()){
			dir.mkdirs();
		}
		File file = new File("data/" + queueName);
		if (!file.exists()) {
			file.createNewFile();
		}
		// 将message消息写入到指定queueName文件，一个queue对应一个文件
		// 先写消息的长度，然后写消息的内容，多行
		//************************
		try{
			OutputStream os = new FileOutputStream(file,true);

			// 写入 4字节长度的byte数组，内容为消息长度
			os.write(ByteBuffer.allocate(4).putInt(message.length).array());

			// 写入消息内容
			os.write(message);

			os.close();
		}
		catch (IOException e){
			System.out.println("Exception");
		}
		
		
		
		
		//************************
	}
}

class Consumer {

	public Collection<byte[]> get(String queueName, long offset, long num) throws NumberFormatException, IOException {
		File file = new File("data/" + queueName);
		if (!file.exists()) {
			return IO.EMPTY;
		}
		Collection<byte[]> result = new ArrayList<>();
		// 一个queue对应一个文件，从指定queue文件中从offset下标开始读取num条的消息
		//************************
		try{
			FileInputStream is = new FileInputStream(file);
			byte[] temp = new byte[20];
			int length = 0;
			for(int i=0;i<offset+num-1;i++)
			{
				// 读取message长度
				int flag = is.read(temp,0,4);
				if (flag == -1)
					break;
				else
				{
					length = (int) (( (temp[0] & 0xFF)<<24)|((temp[1] & 0xFF)<<16)
							|((temp[2] & 0xFF)<<8)|(temp[3] & 0xFF));
				}

				// 读取message
				flag = is.read(temp,0,length);
				if (flag == -1)
					break;
				// message位置大于offset时，将message复制到mes数组中，再加入result
				if(i>=offset)
				{
					byte[] mes = new byte[length];
					System.arraycopy(temp,0, mes ,0, length);
					result.add(mes);
				}

			}
			is.close();
		}
		catch (IOException e){
			System.out.print("Exception");
		}

		
		
		//************************
		return result;
	}
}
