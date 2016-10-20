package com.asiainfo.codis;

import codis.Conf;
import com.asiainfo.codis.action.Assembly;
import com.asiainfo.codis.client.ClientToCodis;
import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.event.EventFactory;
import com.asiainfo.codis.event.EventQueue;
import com.asiainfo.codis.event.OutputFileEvenQueueImpl;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;


import java.io.File;

import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


public class ExportData {
    private static Logger logger = Logger.getLogger(ExportData.class);

    public static void main(String[] args) throws Exception {

        String confDir = Paths.get(ExportData.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().getParent() + File.separator + "conf" + File.separator;

        String logDir = Paths.get(ExportData.class.getProtectionDomain().getCodeSource().getLocation().getPath()).getParent().getParent() + File.separator + "logs";

        System.setProperty("codis_export_log_path", logDir);


        DOMConfigurator.configure(confDir + "log4j.xml");

        StatisticalTablesConf.init();

        String[] codisHostsInfo = Conf.getProp("codisHostsInfo").split(",");
        ForkJoinPool pool = new ForkJoinPool(Conf.getInt(Conf.CODIS_CLIENT_THREAD_COUNT, Conf.DEFAULT_CODIS_CLIENT_THREAD_COUNT));

        EventQueue<List<String>> eventQueue = new OutputFileEvenQueueImpl();
        ExecutorService fixedThreadPool = Executors.newFixedThreadPool(1);

        if (Conf.getBoolean(Conf.EXPORT_FILE_ENABLE, Conf.DEFAULT_EXPORT_FILE_ENABLE)) {
            fixedThreadPool.execute(new EventFactory(eventQueue));
        }

        long startTime = System.currentTimeMillis();

        ClientToCodis clientToCodis = new ClientToCodis(codisHostsInfo, pool, eventQueue);
        ForkJoinTask<Map<String, Map<String, Long>>> result = pool.submit(clientToCodis);

        Map<String, Map<String, Long>> finalResult = result.join();

        if (result.getException() != null) {
            logger.error(result.getException());
        }

        logger.info("All tasks have been done.");
        StatisticalTablesConf.isAllDone = true;

        fixedThreadPool.shutdown();
        exportData(finalResult);
        long endTime = System.currentTimeMillis();
        logger.info("Take " + (endTime - startTime) + "ms.");
    }


    public static void exportData(Map<String, Map<String, Long>> finalResult) {
        logger.info("Start to export data...");
        try {
            SimpleDateFormat postfixFormat = new SimpleDateFormat("yyyy-MM-dd");
            String startTimeStr = postfixFormat.format(new Date()) + " 00:00";
            long startTimeLong = postfixFormat.parse(startTimeStr).getTime();

            for (Map.Entry<String, Map<String, Long>> entry : finalResult.entrySet()) {
                SimpleDateFormat dateFormat = new SimpleDateFormat(StatisticalTablesConf.getAllTablesSchema().get(entry.getKey()).getDateFormat());
                SimpleDateFormat timeFormat = new SimpleDateFormat(StatisticalTablesConf.getAllTablesSchema().get(entry.getKey()).getTimeFormat());
                Date date = new Date();

                SimpleDateFormat currentFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm");
                String currentTimeStr = currentFormat.format(date);
                long currentTimeLong = currentFormat.parse(currentTimeStr).getTime();

                String filePostfix = String.valueOf((currentTimeLong - startTimeLong) / 1000 / Conf.getLong(Conf.CODIS_EXPORT_INTERVAL_S, Conf.DEFAULT_CODIS_EXPORT_INTERVAL_S));

                String filePath = "result" + File.separator + entry.getKey() + "-" + postfixFormat.format(date) + "_" + filePostfix + StatisticalTablesConf.TABLE_FILE_TYPE;

                Class newoneClass = Class.forName(StatisticalTablesConf.getAllTablesSchema().get(entry.getKey()).getHandlerClass());
                Assembly assembly = (Assembly) newoneClass.newInstance();

                List<String> list = new ArrayList();

                Map<String, Long> rows = entry.getValue();

                for (String key : rows.keySet()) {
                    logger.debug("The header is <" + key + ">");
                    list.add(assembly.execute(dateFormat.format(date), timeFormat.format(date), key, String.valueOf(rows.get(key))));
                }

                OutputFileUtils.exportToLocal(filePath, list);
            }
        } catch (Exception e) {
            logger.error("Export data failed.", e);
        }
    }

}