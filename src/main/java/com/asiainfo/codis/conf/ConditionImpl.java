package com.asiainfo.codis.conf;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Created by peng on 16/9/7.
 */
public class ConditionImpl extends Condition{
    private static Logger logger = Logger.getLogger(ConditionImpl.class);
    private Context context;

    @Override
    public synchronized boolean matches(String value) {
        if (StringUtils.equals(getSign(), Condition.AND)) {
            context = new Context(new AndStrategy(this, value));
        } else if (StringUtils.equals(getSign(), Condition.OR)) {
            context = new Context(new OrStrategy(this, value));
        } else {
            return false;
        }
        return context.operate();
    }

    public ConditionImpl deepClone() {
        ConditionImpl cloneObj = null;
        try {
            //写入字节流
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream obs = new ObjectOutputStream(out);
            obs.writeObject(this);
            obs.close();

            //分配内存，写入原始对象，生成新对象
            ByteArrayInputStream ios = new ByteArrayInputStream(out.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(ios);
            //返回生成的新对象
            cloneObj = (ConditionImpl) ois.readObject();
            ois.close();
        } catch (Exception e) {
            logger.error(e);
        }
        return cloneObj;
    }
}
