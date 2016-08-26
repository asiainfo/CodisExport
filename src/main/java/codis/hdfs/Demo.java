package codis.hdfs;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.asiainfo.codis.util.HDFSUtil;

public class Demo implements Runnable {

	@Override
	public void run() {
		boolean result = false;
		try {
			Configuration conf = new Configuration();
			conf.set("fs.defaultFS", "hdfs://sparkstreaming");
		   conf.set("dfs.nameservices", "sparkstreaming");
		   conf.set("dfs.ha.namenodes.sparkstreaming", "nn1,nn2");
		   conf.set("dfs.namenode.rpc-address.sparkstreaming.nn1", "streaming01:8030");
		   conf.set("dfs.namenode.rpc-address.sparkstreaming.nn2", "streaming03:8030");
		   conf.set("dfs.client.failover.proxy.provider.sparkstreaming", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		   conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
			   
			result = HDFSUtil.createDirectory(conf, "/fust/"+Math.random());
			System.out.println(result);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		
	}

}
