package com.ai.thread;

import com.ai.kafka.ScalaKafkaProducer;

/**
 * Created by Jason on 2019/1/1.
 */
public class ThreadProducer implements Runnable {

    @Override
    public void run(){
        ScalaKafkaProducer.produce();
    }

}
