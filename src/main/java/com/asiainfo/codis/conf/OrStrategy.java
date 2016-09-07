package com.asiainfo.codis.conf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.util.regex.Pattern;

/**
 * Created by peng on 16/9/7.
 */
public class OrStrategy implements IStrategy{
    private Condition condition;
    private String value;

    public OrStrategy(Condition condition, String value) {
        this.condition = condition;
        this.value = value;
    }
    @Override
    public boolean operate() {

        int trueCount = 0;

        if (condition.getLike() != null){
            if (Pattern.compile(condition.getLike()).matcher(String.valueOf(value)).matches()){
                trueCount++;
            }
        }

        if (StringUtils.equals(value, StatisticalTablesConf.EMPTY_VALUE)){
            return false;
        }

        if (condition.getEqString() != null){
            if (StringUtils.equals(condition.getEqString(), value)){
                trueCount++;
            }
        }


        if (condition.getNotEqString() != null){
            if (!StringUtils.equals(condition.getNotEqString(), value)){
                trueCount++;
            }
        }


        if (NumberUtils.isNumber(value)){

            double _value = NumberUtils.toDouble(value);

            if (condition.getLess() != Double.MAX_VALUE && _value < condition.getLess()){
                trueCount++;
            }

            if (condition.getLessEqual() != Double.MAX_VALUE && _value < condition.getLessEqual()){
                trueCount++;
            }

            if (condition.getMore() != Double.MIN_VALUE && _value > condition.getMore()){
                trueCount++;
            }

            if (condition.getMoreEqual() != Double.MIN_VALUE && _value > condition.getMoreEqual()){
                trueCount++;
            }


            if (condition.getEqual() != Double.MAX_VALUE){
                if (_value == condition.getEqual()){
                    trueCount++;
                }
            }

            if (condition.getEqual() != Double.MAX_VALUE){
                if (_value != condition.getNotEqual()){
                    trueCount++;
                }
            }

        }


        return trueCount > 0;
    }
}
