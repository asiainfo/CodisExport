package com.asiainfo.codis.conf;

import org.apache.commons.lang.StringUtils;

/**
 * Created by peng on 16/9/7.
 */
public class Context{
    private IStrategy strategy;

    public Context(Condition condition) {
        this.condition = condition;
    }

    private Condition condition;

    public Context(IStrategy strategy){
        this.strategy = strategy;
    }

    public boolean matches(String value){
        if (StringUtils.equals(condition.getSign(), Condition.AND)) {
            strategy = new AndStrategy(condition, value);
        } else if (StringUtils.equals(condition.getSign(), Condition.OR)) {
            strategy = new OrStrategy(condition, value);
        } else {
            return false;
        }
        return this.strategy.operate();
    }
}
