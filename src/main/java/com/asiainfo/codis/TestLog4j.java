package com.asiainfo.codis;

import codis.Conf;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.xml.DOMConfigurator;

import java.io.File;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

/**
 * Created by peng on 16/8/23.
 */
public class TestLog4j {

    public static void main(String[] args) {
        int threadNum = (int)Math.ceil(50*1.0/2);

        String userdir = System.getProperty("user.dir") + File.separator
                + "conf" + File.separator;




        //获取所有的codis集群的slave的ip和port信息，配置到了配置文件里
        String[] codisHostsInfo = Conf.getProp("codisHostsInfo").split(",");

        CountDownLatch countDownLatch;//定义一个静态的CountDownLatch

        //System.out.println("Start time : " + new Date());
        long startTime=System.currentTimeMillis();

        countDownLatch = new CountDownLatch(2);

        //System.out.println("Start to send message...");
        for(String v : codisHostsInfo){
            String[] ipPort = v.split(":");
        new Thread(new GenerateTestData(ipPort[0], Integer.valueOf(ipPort[1]), countDownLatch)).start();
        }

        long firstEndTime=System.currentTimeMillis();
        System.out.println("Finish all messages." + (firstEndTime-startTime)+"ms");


        try {
            countDownLatch.await();   //等待子线程全部执行结束（等待CountDownLatch计数变为0）
            long endTime = System.currentTimeMillis(); //获取结束时间
            System.out.println("End time : " + new Date());
            System.out.println("程序运行时间 " + (endTime-startTime)+"ms");

        } catch (InterruptedException e) {
            System.exit(1);
            e.printStackTrace();
        }



//        String userdir = System.getProperty("user.dir") + File.separator
//                + "conf" + File.separator;
//        //PropertyConfigurator.configure("/Users/peng/SandBox/Dev/Stream/codis-export-data/conf/log4j.xml");
//
//        DOMConfigurator.configure(userdir + "log4j.xml");//加载.xml文件
//
//        Logger LOG = Logger.getLogger(TestLog4j.class);
//        int i = 0;
//        while (i <= 0){
//            LOG.debug("Debug-");
//            LOG.info("Info-");
//            LOG.warn("Warn-");
//            LOG.error("Error-");
//            LOG.fatal("Fatal-");
//
//            i++;
//        }
//
//        Date date = new java.util.Date();
//
//        SimpleDateFormat dateFm = new SimpleDateFormat("yyyyMMdd"); //格式化当前系统日期
//        String dateTime = dateFm.format(date);
//        System.out.println(dateTime);
//
//        Timestamp ts = new Timestamp(System.currentTimeMillis());
//        String tsStr = "";
//        SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
//        try {
//            //方法一
//            tsStr = sdf.format(ts);
//            System.out.println(tsStr);
//            //方法二
//            tsStr = ts.toString();
//            System.out.println(tsStr);
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//


    }
}
