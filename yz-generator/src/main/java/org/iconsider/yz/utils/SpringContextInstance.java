package org.iconsider.yz.utils;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * Created by liuzhenxing on 2017-6-23.
 */
public class SpringContextInstance {
    private static ApplicationContext instance;

    protected SpringContextInstance() {
    }

    public static ApplicationContext getInstance() {
        if (null == instance) {
            instance = new ClassPathXmlApplicationContext("applicationContext.xml");
        }
        return instance;
    }

    public static <T> T getBean(String id, Class<T> requiredType) {
        ApplicationContext context = getInstance();
        if (context != null) {
            return context.getBean(id, requiredType);
        }
        return null;
    }
}
