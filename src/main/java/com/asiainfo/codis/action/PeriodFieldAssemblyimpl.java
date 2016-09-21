package com.asiainfo.codis.action;

import com.asiainfo.codis.conf.StatisticalTablesConf;
import org.apache.log4j.Logger;

/**
 * Created by peng on 16/9/21.
 */
public class PeriodFieldAssemblyimpl implements Assembly {
    private static Logger logger = Logger.getLogger(PeriodFieldAssemblyimpl.class);
    @Override
    public String execute(String date, String time, String col, String count) {
        String[] date_time = time.split(" ");
        String [] period_min = date_time[1].split(":");

        StringBuilder result = new StringBuilder();
        result.append(date)
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(Integer.parseInt(period_min[0].trim()))
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(Integer.parseInt(period_min[1].trim()))
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(time)
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(col)
                .append(StatisticalTablesConf.TABLE_COLUMN_SEPARATOR)
                .append(count);

        return result.toString();
    }
}
