package zktools;

import java.security.NoSuchAlgorithmException;

import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

public class DigestTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		try {
			System.out.println(DigestAuthenticationProvider.generateDigest("zzh:ABC_abc1"));
		} catch (NoSuchAlgorithmException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
