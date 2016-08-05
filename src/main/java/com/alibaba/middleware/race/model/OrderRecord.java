package com.alibaba.middleware.race.model;

public class OrderRecord extends Record {
    long orderId;
    byte[] record;

    public OrderRecord(long id, byte[] record) {
        super(id, record);
        this.orderId = id;
        this.record = record;
    }
}