package com.asiainfo.codis.event;

import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.log4j.Logger;

/**
 * Created by peng on 16/9/6.
 */
public class EventFactory implements Runnable {
    private static Logger logger = Logger.getLogger(EventFactory.class);
    private EventQueue eventQueue;

    public EventFactory(EventQueue eventQueue) {
        this.eventQueue = eventQueue;
    }


    @Override
    public void run() {

        while (true) {

            if (eventQueue.consumeEvent() && StatisticalTablesConf.isAllDone) {
                if (!eventQueue.isCacheEmpty()){
                    logger.info("No new event, write last batch");
                    eventQueue.flushData();
                }
                break;
            }
            else {
                logger.debug("Left " + eventQueue.getEventsCounts() + " files copied to HDFS.");
            }

        }

        logger.info("All files had been persisted to local and HDFS.");
        OutputFileUtils.close();
    }
}
