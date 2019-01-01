package com.ai.thread;

/**
 * Created by Jason on 2019/1/1.
 */
public class ThreadProducerConsumer {

    public static void run(){
        ThreadConsumer consumer = new ThreadConsumer();
        ThreadProducer producer = new ThreadProducer();

        Thread threadConsumer = new Thread(consumer);
        Thread threadProducer = new Thread(producer);

        threadProducer.start();
        threadConsumer.start();
    }
}
