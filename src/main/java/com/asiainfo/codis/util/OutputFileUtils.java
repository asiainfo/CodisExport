package com.asiainfo.codis.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.List;


public class OutputFileUtils {

    private static final String OUT_BASE_DIR = "tables" + File.separator;

    static {
        File baseDir = new File(OUT_BASE_DIR);
        if (!baseDir.exists()){
            baseDir.mkdir();
        }
    }

    public static void exportToLocal(String fileName, List<String> datas){
        FileOutputStream outStr ;
        BufferedOutputStream buf = null;
        try {
            outStr = new FileOutputStream(new File(OUT_BASE_DIR + fileName));
            buf = new BufferedOutputStream(outStr);
            for (String data : datas){
                buf.write((data + System.getProperty("line.separator", "\n")).getBytes());
            }
            buf.flush();
            buf.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            try {
                if (buf != null){
                    buf.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

    }

    public static void exportToHDFS(File targetDir){

    }

    public static void exportToHDFS(String localFilePath, String hdfsPath){
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        Configuration conf = new Configuration();

        conf.addResource(new Path(userdir + File.separator + "hdfs-site.xml"));
        conf.addResource(new Path(userdir + File.separator + "core-site.xml"));


        try {
            if (!HDFSUtil.exits(conf, hdfsPath)){
                HDFSUtil.createDirectory(conf, hdfsPath);
            }

            HDFSUtil.copyFromLocalFile(conf, localFilePath, hdfsPath);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        }
    }
}
