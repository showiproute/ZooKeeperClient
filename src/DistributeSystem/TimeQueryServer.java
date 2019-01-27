package DistributeSystem;

import java.io.IOException;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs.Ids;

public class TimeQueryServer {

	ZooKeeper zk=null;
	//构造zk客户端连接
	public void connectZk() throws IOException {

		zk = new ZooKeeper("namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181",
				2000,null);	
	}
	
	//注册服务器信息
	public void registerServerInfo(String hostname,String port) throws KeeperException, InterruptedException {
		
		//先判断注册节点的父节点是否存在，如果不存在，则创建
		
		Stat stat = zk.exists("/servers", false);
		if(stat==null) {
			zk.create("/servers/server",null, Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		}
		//注册服务器数据到zk的约定注册节点下
//		String create=zk.create("/servers/server",(hostname+":"+port).getBytes() , Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
		zk.setData("/servers/server", (hostname+":"+port).getBytes(), -1);
		
		System.out.println(hostname+"服务器向zk注册信息成功，注册的节点为:/servers/server");
	}
	

	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
		
		TimeQueryServer timeQueryServer = new TimeQueryServer();
		
		//构造zk客户端连接
		timeQueryServer.connectZk();
		
		//注册服务器信息
		timeQueryServer.registerServerInfo(args[0], args[1]);
		
		//启动业务线程开始处理业务
		new TimeQueryService(Integer.parseInt(args[1])).start();
		
		
	}
	
}
