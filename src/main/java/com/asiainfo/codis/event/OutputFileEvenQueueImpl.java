package com.asiainfo.codis.event;

import codis.Conf;
import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.commons.collections.list.CursorableLinkedList;
import org.apache.log4j.Logger;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by peng on 16/9/6.
 */
public class OutputFileEvenQueueImpl extends EventQueue<List<String>>{
    private static Logger logger = Logger.getLogger(OutputFileEvenQueueImpl.class);
    private int MAX_CACHE_SIZE = Conf.getInt(Conf.HDFS_OUTPUT_COUNTS_PER_FILE, Conf.DEFAULT_HDFS_OUTPUT_COUNTS_PER_FILE);

    private List<String> cache = new ArrayList<>();
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
            event = events.take();
        } catch (InterruptedException e) {
            logger.error("Unknown error", e);
        }

        cache.addAll(event);

        if (cache.size() > MAX_CACHE_SIZE){
            flushData();
        }

        return events.isEmpty();
    }

    @Override
    public boolean produceEvent(List<String> event) {
        try {
            events.put(event);
        } catch (Exception e) {
            logger.error("Unknown error", e);
            return false;
        }

        return true;
    }

    @Override
    public void flushData(){
        long startTime=System.currentTimeMillis();
        String fileName = "codis-" + String.valueOf(System.currentTimeMillis()) + StatisticalTablesConf.TABLE_FILE_TYPE;
        OutputFileUtils.exportToLocal(("source" + File.separator + fileName), cache);

        long endLocalTime=System.currentTimeMillis();
        logger.info("Export " + cache.size() + " data to local taking " + (endLocalTime - startTime) + "ms.");
        logger.debug("Start to export data to hdfs ...");
        OutputFileUtils.exportToHDFS(fileName);

        long endHdfsTime=System.currentTimeMillis();
        logger.info("Export " + cache.size() + " data to HDFS taking " + (endHdfsTime - endLocalTime) + "ms.");
        cache.clear();

    }

    @Override
    public boolean isCacheEmpty() {
        return cache.isEmpty();
    }

}
