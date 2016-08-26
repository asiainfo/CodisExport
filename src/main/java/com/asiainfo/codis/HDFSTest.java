package com.asiainfo.codis;

import com.asiainfo.codis.util.HDFSUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;

/**
 * Created by peng on 16/8/23.
 */
public class HDFSTest {
    static Logger logger = Logger.getLogger(HDFSTest.class);
    public static void main(String[] args) {

        logger.info("I am here...");
        Configuration conf = new Configuration();

        String userdir = args[0];

        if(StringUtils.isBlank(userdir)){
            System.err.println("No args");
            System.exit(0);
		}

        System.out.println("conf dir is " + userdir);

//        //System.out.println(f.getAbsolutePath());
        conf.addResource(new Path(userdir + File.separator + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + File.separator + "core-site.xml"));

String testPath = "/tmp/testt.txt";

        try {
            if (HDFSUtil.exits(conf, testPath)){
                System.out.println("Here");

                HDFSUtil.deleteFile(conf, testPath);
                HDFSUtil.createFile(conf, testPath, "Hello Hadoop");

            }else {
                HDFSUtil.createFile(conf, testPath, "Hello Spark");
            }


            System.out.println(HDFSUtil.readFile(conf, testPath));
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }





        System.out.println("====");
    }
}
