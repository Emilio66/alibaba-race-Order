package com.alibaba.middleware.race.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by zhaoz on 2016/7/29.
 */
public class ThreadPoolTest {
    public static void main(String args[]){
        ExecutorService executor = Executors.newFixedThreadPool(3);
        for (int i = 0; i < 6; i++) {
            executor.execute(new Runnable() {
                @Override
                public void run() {
                    int cnt = 0;
                    while(cnt ++ < 5) {
                        System.out.println("Thread " + Thread.currentThread().getName() + " count: " + cnt);
                        try {
                            Thread.sleep(100);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }
            });
        }

        executor.shutdown();    //close executor after all work done
        try {
            long start = System.nanoTime();
            executor.awaitTermination(10, TimeUnit.MINUTES);
            System.out.println("Haven been waiting for: "+(System.nanoTime() - start));
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
