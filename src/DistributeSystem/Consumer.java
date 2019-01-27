package DistributeSystem;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;

public class Consumer {
	
	//定义一个list用于存放最新的在线服务器列表
	private ArrayList<String> onlineSevers=new ArrayList<>();
	
	ZooKeeper zk=null;
	//构造zk连接对象
	public void connectZk() throws IOException {

		zk = new ZooKeeper("namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181",
				2000,new Watcher() {
			@Override
			public void process(WatchedEvent event) {
				// TODO Auto-generated method stub
				if(event.getState() == KeeperState.SyncConnected &&
						event.getType() == EventType.NodeChildrenChanged) {
					try {
						//事件回调逻辑中，再次查询zk上的在线服务器节点即可，查询逻辑中再次注册子节点变化事件监听
						getOnlineServers();
					} catch (Exception e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});	
	}
	
	//查询在线服务器列表
	public void getOnlineServers() throws Exception{

		List<String> children = zk.getChildren("/servers", true);
		ArrayList<String> servers = new ArrayList<>();
		
		for (String child : children) {
			byte[] data = zk.getData("/servers/"+child, false, null);
			String serverInfo = new String(data);
						
			servers.add(serverInfo);
		}
		onlineSevers=servers;
		
		System.out.println("查询了一次zk,当前在线的服务器有:"+servers);
		
	}
	
	//处理业务(向一台服务器发送时间查询请求)
	public void sendRequest() throws Exception{
//		System.out.println("消费者开始处理业务功能.....");	

//		Thread.sleep(Long.MAX_VALUE);
		
		Random random = new Random();
		//挑选一台当前在线的服务器
		while(true) {		
			int nextInt = random.nextInt(onlineSevers.size());
			String server=onlineSevers.get(nextInt);
			String hostname=server.split(":")[0];
			int port=Integer.parseInt(server.split(":")[1]);
			
			System.out.println("本次请求挑选的服务器为:"+server);

			Socket socket = new Socket(hostname,port);
			OutputStream outputStream = socket.getOutputStream();
			InputStream inputStream = socket.getInputStream();
			outputStream.write("haha".getBytes());
			outputStream.flush();

			byte[] buf=new byte[256];
			int read = inputStream.read(buf);
			
			System.out.println("服务器 响应的时间为："+new String(buf,0,read));
			
			outputStream.close();
			inputStream.close();
			socket.close();
			
			Thread.sleep(10000);
			
		}
		
	}
	
	
	
	public static void main(String[] args) throws Exception{
	
		Consumer consumer = new Consumer();

		//构造zk连接对象
		consumer.connectZk();
		
		//查询在线服务器列表
		consumer.getOnlineServers();
		
		//处理业务(向一台服务器发送时间查询请求)
		consumer.sendRequest();
		
	}
	
}
