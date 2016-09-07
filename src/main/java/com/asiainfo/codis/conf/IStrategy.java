package com.asiainfo.codis.conf;

import java.io.Serializable;

/**
 * Created by peng on 16/9/7.
 */
public interface IStrategy extends Serializable {
    boolean operate();
}
