package com.alibaba.middleware.race.model;

/**
 * Created by zhaoz on 2016/7/31.
 * no need to use hashmap entry, just use simple object
 */
public class IndexPair {
    public long K;
    public long V;
    public long confNum;
    public IndexPair(long K, long V){
        this.K = K;
        this.V = V;
        confNum = 0L;
    }
}
