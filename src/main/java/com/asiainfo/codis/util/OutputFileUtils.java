package com.asiainfo.codis.util;

import codis.Conf;
import com.asiainfo.codis.ExportData;
import io.netty.util.internal.StringUtil;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.Logger;

import java.io.*;
import java.nio.file.Paths;
import java.util.List;


public class OutputFileUtils {

    private static Logger logger = Logger.getLogger(OutputFileUtils.class);
    private static String OUT_LOCAL_BASE_DIR = "tables" + File.separator;
    private static final String HDFS_DEFAULT_OUTPUT_PATH = "/tmp/codis";
    private static Configuration conf;
    private static FileSystem fs;

    private static String hdfsOutputPath;

    static {
        String baseDir;

        if (StringUtils.isNotEmpty(Conf.getProp(Conf.CODIS_OUTPUT_BASE_PATH))){
            baseDir = Conf.getProp(Conf.CODIS_OUTPUT_BASE_PATH);
        }
        else {
            baseDir = Paths.get(ExportData.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().getParent().toString();
        }

        OUT_LOCAL_BASE_DIR = baseDir + File.separator ;//+ "tables" + File.separator;
        File out_local_base_dir = new File(OUT_LOCAL_BASE_DIR);

        if (!out_local_base_dir.exists()){
            out_local_base_dir.mkdir();
        }

        String confDir = baseDir + File.separator + "conf" + File.separator;


        conf = new Configuration();
        conf.addResource(new Path(confDir + File.separator + "hdfs-site.xml"));
        conf.addResource(new Path(confDir + File.separator + "core-site.xml"));
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");

        if (Conf.getBoolean(Conf.EXPORT_FILE_ENABLE, Conf.DEFAULT_EXPORT_FILE_ENABLE)){
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                logger.error(e.getMessage());
            }
        }

    }

    public static void exportToLocal(String fileName, List<String> datas){
        FileOutputStream outStr ;
        BufferedOutputStream buf = null;
        try {
            File outputFile = new File(OUT_LOCAL_BASE_DIR + fileName);
            if (!outputFile.getParentFile().exists()){
                outputFile.getParentFile().mkdir();
            }

            outStr = new FileOutputStream(new File(OUT_LOCAL_BASE_DIR + fileName));

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

    public static void exportToHDFS(String localFileName){
        try {

            Path localPath = new Path(OUT_LOCAL_BASE_DIR + localFileName);
            Path remotePath = new Path(getOutputHdfsPath());

            //TODO the first arg true or false?
            fs.copyFromLocalFile(false, true, localPath, remotePath);
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void init() {
        Path hdfsPath = new Path(getOutputHdfsPath());
        try {
            if (!fs.exists(hdfsPath)) {
                fs.mkdirs(hdfsPath);
            }
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void close(){
        try {
            fs.close();
        } catch (IOException e) {
            logger.error(e.getMessage());
        }
    }

    public static void setHdfsOutputPath(String hdfsOutputPath) {
        OutputFileUtils.hdfsOutputPath = hdfsOutputPath;
    }

    private static String getOutputHdfsPath(){
        return StringUtils.isEmpty(hdfsOutputPath) ? HDFS_DEFAULT_OUTPUT_PATH : hdfsOutputPath;
    }

}
