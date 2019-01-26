package ZookeeperClientDemo;

import org.apache.zookeeper.ZooKeeper;

import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs.Ids;
import org.junit.Before;
import org.junit.Test;

public class ZooKeeperClientDemo {

	ZooKeeper zk=null;
	
	@Before
	public void init() throws Exception{
		zk = new ZooKeeper("namenode:2181,datanode1:2181,datanode2:2181,datanode3:2181",
				2000,null);
	}
	
	
	@Test
	public void testCreate() throws Exception{
		

		//参数1： 要创建的节点路径 2：数据 3：权限 4：节点类型
		String create = zk.create("/eclipse", "hello eclipse".getBytes(), Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
	
		System.out.println(create);
		
		zk.close();
		
	}
	
	@Test
	public void testUpdate() throws Exception{
	
		//参数3： 所要修改的版本，-1代表任何版本
		zk.setData("/eclipse", "你好".getBytes("UTF-8"), -1);
	
		zk.close();
	}
	
	@Test
	public void testGet() throws Exception{
	
		
		//参数2 ： 是否要监听 watch 3：null表示最新版本
		byte[] data = zk.getData("/eclipse", false,null);
		
		System.out.println(new String(data,"UTF-8"));
	
		zk.close();
	}
	
	@Test
	public void testListChildren() throws Exception{
	
		
		List<String> children = zk.getChildren("/eclipse", false,null);
		
		for (String child : children) {
			System.out.println(child);
		}
	
		zk.close();
	}
	
	@Test
	public void rmDir() throws Exception{
		
		zk.delete("/eclipse", -1);
		zk.close();
	}
	
	
	
}
