package com.asiainfo.codis.conf;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;

import java.util.regex.Pattern;

/**
 * Created by peng on 16/9/7.
 */
public class AndStrategy implements IStrategy{
    private Condition condition;
    private String value;

    public AndStrategy(Condition condition, String value) {
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
        else {
            trueCount++;
        }


        if (StringUtils.equals(value, StatisticalTablesConf.EMPTY_VALUE)){
            return false;
        }

        if (condition.getEqString() != null){
            if (StringUtils.equals(condition.getEqString(), value)){
                trueCount++;
            }
        }
        else {
            trueCount++;
        }

        if (condition.getNotEqString() != null){
            if (!StringUtils.equals(condition.getNotEqString(), value)){
                trueCount++;
            }
        }
        else {
            trueCount++;
        }

        if (NumberUtils.isNumber(value)){
            double _value = NumberUtils.toDouble(value);
            if (_value < condition.getLess()
                    && _value <= condition.getLessEqual()
                    && _value > condition.getMore()
                    && _value >= condition.getMoreEqual()){
                trueCount++;
            }


            if (condition.getEqual() != Double.MAX_VALUE){
                if (_value == condition.getEqual()){
                    trueCount++;
                }
            }else {
                trueCount++;
            }

            if (condition.getEqual() != Double.MAX_VALUE){
                if (_value != condition.getNotEqual()){
                    trueCount++;
                }
            }else {
                trueCount++;
            }

            return trueCount==6;
        }


        return trueCount==3;
    }

}
