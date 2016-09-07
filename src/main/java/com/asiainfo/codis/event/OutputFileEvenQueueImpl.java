package com.asiainfo.codis.event;

import codis.Conf;
import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by peng on 16/9/6.
 */
public class OutputFileEvenQueueImpl extends EventQueue<List<String>>{
    private static Logger logger = Logger.getLogger(OutputFileEvenQueueImpl.class);
    public OutputFileEvenQueueImpl() {
        String hdfsOutputPath = Conf.getProp("hdfs.output.path");
        if (Conf.getBoolean(Conf.EXPORT_FILE_ENABLE, Conf.DEFAULT_EXPORT_FILE_ENABLE)){
            OutputFileUtils.setHdfsOutputPath(hdfsOutputPath);
            OutputFileUtils.init();
        }
    }

    @Override
    public boolean consumeEvent() {
        List<String> event = null;
        logger.debug("Start to export data to local ...");
        try {
            event = events.take();// 从盘子开始取一个鸡蛋，如果盘子空了，当前线程阻塞
        } catch (InterruptedException e) {
            logger.error(e);
        }

        String fileName = "codis-" + String.valueOf(System.currentTimeMillis()) + StatisticalTablesConf.TABLE_FILE_TYPE;
        OutputFileUtils.exportToLocal(fileName, event);
        logger.debug("Start to export data to hdfs ...");
        OutputFileUtils.exportToHDFS(fileName);

        return events.isEmpty();
    }

    @Override
    public boolean produceEvent(List<String> event) {
        try {
            events.put(event);// 向盘子末尾放一个鸡蛋，如果盘子满了，当前线程阻塞
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }


}
