package com.alibaba.middleware.race.model;

import sun.nio.ch.DirectBuffer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaoz on 2016/7/27.
 *
 * maintain relation between buyer/good with orders, including
 *
 * 1. buyer/order's record store position, i.e. offset
 * 2. order list attached to this object (saved in direct memory)
 *
 */
public class RelationOperator implements Comparable{
    private int num;
    public int offset;     //offset of buyer or good in data file
    public int indexOff;   //offset in index file, store in memory
    public int writtenNum;      //how many order has been saved in file
    public List<Long> orderList;
    //private ByteBuffer ordersBuffer;  //not now, put in off-heap memory

    public RelationOperator(int defaultNum){
        this.num = defaultNum;
        this.orderList = new ArrayList<Long>(defaultNum);
        this.indexOff = Integer.MAX_VALUE;  //not write to disk yet, append on file
        this.writtenNum = 0;
        this.offset = -1;   //for judge
    }

    @Override
    public String toString() {
        return "RelationOperator{" +
                "offset=" + offset +
                ", indexOff=" + indexOff +
                ", writtenNum=" + writtenNum +
                ", orderList=" + orderList +
                '}';
    }

    /**
     * add related order's location to byteBuffer, synchronization was controlled for order list
     * @param orderLoc
     */
    public void add(long orderLoc) {

        synchronized (orderList) {
            if (orderList.size() < num) {
                orderList.add(orderLoc);

            } else {
                //To do: overflow, add to memory

            }
        /*if(ordersBuffer == null){
            ordersBuffer = ByteBuffer.allocateDirect(64 * num); //use off-heap memory

        }else if(ordersBuffer.remaining() < 64){
            //enlarge buffer
            ByteBuffer tmpBuffer = ByteBuffer.allocateDirect(ordersBuffer.capacity() + 2 * 64);
            ordersBuffer.position(0);
            tmpBuffer.put(ordersBuffer);
            ordersBuffer = tmpBuffer;
        }

        ordersBuffer.putLong(orderLoc);*/
        }
    }

    /**
     * Read file, get all location list
     * @return
     */
    /*public List<Long> getLocList(){

        *//*if(ordersBuffer != null){
            List<Long> locList = new ArrayList<Long>();
            ordersBuffer.flip();    //ready to output
            while(ordersBuffer.hasRemaining()){
                locList.add(ordersBuffer.getLong());
            }
            return  locList;
        }*//*
        return this.orderList;
    }*/


    /**
     * sorted by index offset, for writing index file sequentially
     * @param o
     * @return
     */
    @Override
    public int compareTo(Object o) {
        return indexOff - ((RelationOperator)o).indexOff;
    }
}
