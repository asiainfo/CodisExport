package com.asiainfo.codis.conf;

import java.io.Serializable;

/**
 * Created by peng on 16/9/7.
 */
public class Context implements Serializable {
    private IStrategy strategy;

    public Context(IStrategy strategy){
        this.strategy = strategy;
    }
    public void setStrategy(IStrategy strategy){
        this.strategy = strategy;
    }
    public boolean operate(){
        return this.strategy.operate();
    }
}
