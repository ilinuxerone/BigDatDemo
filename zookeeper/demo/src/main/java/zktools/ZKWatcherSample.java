package zktools;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
/**
 * 可以注册watcher的方法：getData、exists、getChildren。
 *可以触发watcher的方法：create、delete、setData。连接断开的情况下触发的watcher会丢失。
 * @author Administrator
 */
public class ZKWatcherSample implements Watcher {
	
	private static CountDownLatch  connectedSemaphore = new CountDownLatch(1);
	private static ZooKeeper zk = null;
	private static Stat stat = new Stat();
	// 默认配置信息
	private final static String DEFAULT_HOST = "127.0.0.1";
	private final static int DEFAULT_PORT = 2181;
	private final static int DEFAULT_TIMEOUT = 30000;
	@Override
	public void process(WatchedEvent event) {
		// TODO Auto-generated method stub
		System.out.println("receive watched event:"+event);
		if (KeeperState.SaslAuthenticated == event.getState()){
			connectedSemaphore.countDown();
		}
		if (event.getType() == EventType.NodeDataChanged){
			try {
				byte data[] = zk.getData(event.getPath(), true, stat);
				System.out.println("添加节点后="+new String(data));
				System.out.println("添加节点后 dataversion="+stat.getVersion());
				connectedSemaphore.countDown();
			} catch (KeeperException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if(event.getType()==EventType.NodeChildrenChanged){
				List<String> childrenList = null;
				try {
					childrenList = zk.getChildren(event.getPath(), true);
					System.out.println("添加节点后="+childrenList.toString());
					connectedSemaphore.countDown();
				} catch (KeeperException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
			if (KeeperState.SyncConnected == event.getState()) {
				connectedSemaphore.countDown();
			}
			ZKWatcherSample we1 = new ZKWatcherSample();
			we1.setZk(zk);
		}
	}
	
	public ZooKeeper getZk() {
		return zk;
	}
	public void setZk(ZooKeeper zk) {
		this.zk = zk;
	}
	public static void main(String[] args) throws IOException, KeeperException, InterruptedException {
/*		ZooKeeper zookeeper = new ZooKeeper("localhost:2181",6000, new ZKCreateSample());
		System.out.println("begin state="+zookeeper.getState());
		try {
			connectedSemaphore.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
			System.out.println("Zookeeper session established.");
		}
		System.out.println("end state="+zookeeper.getState());
		*/
		
		String path = "/acl";
/*		zk = new ZooKeeper(DEFAULT_HOST+":"+DEFAULT_PORT,5000,new ZKCreateSample());
		byte data[] = zk.getData(path,true, stat);
		System.out.println("before="+new String(data));
		System.out.println("before dataversion="+stat.getVersion());
		connectedSemaphore.await();*/
		
/*		zk = new ZooKeeper("localhost:2181",5000,new ZKCreateSample());
		List<String> childrenList = zk.getChildren(path, true);
		System.out.println(childrenList.toString());
		connectedSemaphore.await();*/
		
		ZooKeeper zookeeper = new ZooKeeper("localhost:2181", 6000,
				new ZKWatcherSample());
		connectedSemaphore.await();
		long sessionId = zookeeper.getSessionId();
		byte[] passwd = zookeeper.getSessionPasswd();
		Thread.sleep(30000);
		System.out.println("sleep end");
		//zookeeper.;
		//用不合法的sessionID和sessionPasswd创建连接
//		ZooKeeper zookeeper1 = new ZooKeeper("localhost:2181", 6000,
//				new ZKCreateSample_With_SID_PASSWD(),1l,"test".getBytes());
		
		//用合法的sessionID和sessionPasswd创建连接
		ZooKeeper zookeeper2 = new ZooKeeper("localhost:2181", 6000,
				new ZKWatcherSample(),sessionId,passwd);
		
		//Thread.sleep(3000);
		//System.out.println("begin state="+zookeeper2.getState());
		Thread.sleep(30000);
		//System.out.println("end state=" + zookeeper.getState());
	}


}
