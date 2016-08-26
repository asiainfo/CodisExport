package com.asiainfo.codis;

import codis.Conf;
import com.asiainfo.codis.client.ClientToCodis;
import com.asiainfo.codis.conf.StatisticalTablesConf;
import com.asiainfo.codis.util.OutputFileUtils;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;


import java.io.File;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;


public class ExportData {
    private static Logger logger = Logger.getLogger(ExportData.class);

    public static void main(String[] args) throws Exception{
        String userdir = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
        DOMConfigurator.configure(userdir + "log4j.xml");

        String[] codisHostsInfo = Conf.getProp("codisHostsInfo").split(",");

        long startTime=System.currentTimeMillis();

        ForkJoinPool pool = new ForkJoinPool();
        ClientToCodis clientToCodis = new ClientToCodis(codisHostsInfo);
        ForkJoinTask<Map<String, Map<String, Long>>> result = pool.submit(clientToCodis);

        Map<String, Map<String, Long>> finalResult = result.join();

        logger.info("All tasks have been done.");

        exportData(finalResult);

        long endTime = System.currentTimeMillis();
        logger.info("Take " + (endTime - startTime) + "ms.");

    }


    public static void exportData(Map<String, Map<String, Long>> finalResult){
        logger.info("Start to export data...");
        for (Map.Entry entry : finalResult.entrySet()) {
            String filePath = entry.getKey() + StatisticalTablesConf.TABLE_FILE_TYPE;

            List<String> list = new ArrayList<String>();

            Map<String, Long> rows = (Map<String, Long>)entry.getValue();

            for (String key : rows.keySet()){
                list.add(key + String.valueOf(rows.get(key)));
            }

            OutputFileUtils.exportToLocal(filePath, list);
        }
    }

}
