package ZookeeperClientDemo;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperWatchDemo {

	ZooKeeper zk=null;
	
	@Before
	public void init() throws Exception {
		
		zk = new ZooKeeper("namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181",
				2000, new Watcher() {
					@Override
					public void process(WatchedEvent event) {
						// TODO Auto-generated method stub
						if(event.getState() == KeeperState.SyncConnected && 
								event.getType() == EventType.NodeDataChanged) {
							System.out.println(event.getPath());//收到的事件所发生的节点路径
							System.out.println(event.getType());//收到事件的类型
							System.out.println("change it....");//收到事件后，具体处理逻辑
							
							try {
								zk.getData("/mygirls", true, null);
							} catch (KeeperException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							} catch (InterruptedException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
						}else if(event.getState() == KeeperState.SyncConnected &&
								event.getType() == EventType.NodeChildrenChanged) {
							System.out.println("NodeChild change");
						}
					}
				});
	}
	
	@Test
	public void testGetWatch() throws Exception{		
		byte[] data = zk.getData("/mygirls",true, null);
		System.out.println(new String(data,"UTF-8"));
		Thread.sleep(Long.MAX_VALUE);
		
	}
	
	
	
}
