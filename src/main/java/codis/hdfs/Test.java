package codis.hdfs;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class Test {

	public static void main(String[] args) {
		List list = new ArrayList();
		list.add("--AppName=aa");
		list.add("--mysqlName=bb");
		list.add("--sparksql=cc");
		
		Iterator it = list.iterator();
		String appname = "";
		String mysql = "";
		String sparksql = "";
		while(it.hasNext()){
			String kv = (String)it.next();
			if(kv.startsWith("--AppName")){
				appname = kv;
			}else if(kv.startsWith("--mysqlName")){
				mysql = kv;
			}else if(kv.startsWith("--sparksql")){
				sparksql = kv;
			}
		}
		System.out.println(appname);
		System.out.println(mysql);
		System.out.println(sparksql);
		
		
		
//		Thread t1 = new Thread(new Demo());
//		Thread t2 = new Thread(new Demo());
//		t1.start();
//		t2.start();
	}

}
