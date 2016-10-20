package com.asiainfo.codis.action;

import com.asiainfo.codis.conf.StatisticalTablesConf;
import org.apache.log4j.Logger;

/**
 * Created by peng on 16/9/21.
 */
public class AssemblyImpl implements Assembly {
    private static Logger logger = Logger.getLogger(AssemblyImpl.class);
    @Override
    public String execute(String date, String time, String col, String count) {
        StringBuilder result = new StringBuilder();
        result.append(date)
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(time)
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(col)
                .append(count);

        logger.debug("Result str is <" + result + ">");
        return result.toString();
    }
}
