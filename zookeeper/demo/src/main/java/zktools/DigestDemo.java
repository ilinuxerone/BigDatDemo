package zktools;

import java.util.ArrayList;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class DigestDemo {

	  public static void main(String[] args) throws Exception {
	        // TODO Auto-generated method stub
	        //new一个acl
	        List<ACL> acls = new ArrayList<ACL>();
	        //添加第一个id，采用用户名密码形式
	        Id id1 = new Id("digest",
	                DigestAuthenticationProvider.generateDigest("admin:admin"));
	        ACL acl1 = new ACL(ZooDefs.Perms.ALL, id1);
	        acls.add(acl1);
	        //添加第二个id，所有用户可读权限
	        Id id2 = new Id("world", "anyone");
	        ACL acl2 = new ACL(ZooDefs.Perms.READ, id2);
	        acls.add(acl2);

	        // zk用admin认证，创建/test ZNode。

	        ZooKeeper zk = new ZooKeeper(
	                "host1:2181,host2:2181,host3:2181",
	                2000, null);
	        zk.addAuthInfo("digest", "admin:admin".getBytes());
	        zk.create("/test", "data".getBytes(), acls, CreateMode.PERSISTENT);
	   }
}
