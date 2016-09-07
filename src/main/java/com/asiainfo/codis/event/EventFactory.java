package com.asiainfo.codis.event;

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
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            logger.error(e);
        }

        while (true) {

            if (eventQueue.consumeEvent()) {
                break;
            }

        }

        logger.info("All files had been persisted to local and HDFS.");
        OutputFileUtils.close();
    }
}
