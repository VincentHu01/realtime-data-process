package com.ai.utils;

import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Jason on 2019/1/4.
 */
public class PropUtil {
    public static Properties getProps(String fileName){
        Properties prop = new Properties();
        try{
            InputStream inputStream = PropUtil.class.getClassLoader().getResourceAsStream(fileName);
            prop.load(inputStream);
        }catch (Exception ex){
            ex.printStackTrace();
        }
        return prop;
    }
}
