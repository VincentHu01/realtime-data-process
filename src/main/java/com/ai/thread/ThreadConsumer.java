package com.ai.thread;

import com.ai.spark.SparkStreaming;

/**
 * Created by Jason on 2019/1/1.
 */
public class ThreadConsumer implements Runnable {

    @Override
    public void run(){
        System.out.print("start consumer");
        SparkStreaming.run();
    }
}
