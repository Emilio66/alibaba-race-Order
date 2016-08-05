package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.file.FileParser;
import com.alibaba.middleware.race.util.BitHelper;
import com.alibaba.middleware.race.util.FastAppender;
import com.alibaba.middleware.race.util.FastReader;
import com.alibaba.middleware.race.util.RandomWriter;

import java.io.File;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by zhaoz on 2016/7/30.
 * * Matain map <pk,  object[offset, list<order_location>]>
 * 1. in parsing phrase, flush offset info to disk for limited memory
 * 2. after parsing, load file to memory, construct map selectively
 */
public class GoodBuyerBucket {
    private int buffSize;
    private int capacity;                       //number of record in each bucket
    private int tailPos = 0;
    private int initOrderNum;                   //initial order list size for each row

    private volatile int curRecNum = 0;             //current record number, observable by all thread
    private volatile boolean isReady = false;       //whether the new bucket is ready for insertion

    public Map<String, RelationOperator> relationMap;   //<goodId, object[offset, list<order_location>]>
    public ConcurrentHashMap<String, Integer> offsetMap;    //goodId, buyerId & offset
    public String storePath;
    private byte THRESHOD = 10;                 //error allow range
    private byte type;

    public GoodBuyerBucket(byte type, String storePath, int buffSize, int capacity, int initOrderNum) {
        this.type = type;
        this.storePath = storePath;
        this.buffSize = buffSize;
        this.capacity = capacity;
        this.initOrderNum = initOrderNum;
        this.relationMap = new HashMap<String, RelationOperator>((int) (capacity * 1.51));//not grow map for efficiency
        //this.offsetMap = new ConcurrentHashMap<String, Integer>((int) (capacity * 1.4));
    }

    /**
     * put the related order location list to this buyer or good
     *
     * @param pk
     * @param offset
     * @param fileId
     */
    public void putOrderLoc(String pk, long offset, byte fileId) {

        long fId_off = BitHelper.assembleOrderFidOff(fileId, offset);
        RelationOperator operator;

        synchronized (relationMap) {
            operator = relationMap.get(pk);
            if (null == operator) {
                operator = new RelationOperator(initOrderNum);
                relationMap.put(pk, operator);
            }
        }

        operator.add(fId_off);  //change the object, doesn't affect map's reference

    }

    /**
     * put the (32bit) location of this good/buyer in map's value object
     *
     * @param pk
     * @param fileId
     * @param offset
     */
    public void putLoc(String pk, byte fileId, int offset) {

        int fId_off = BitHelper.assembeGBFidOff(fileId, offset);

        //offsetMap.put(pk, fId_off); //concurrent hash map will handle this, TO DO: saved in relationObj
        curRecNum++;

        //dump to disk
        if ((capacity - curRecNum) > THRESHOD) {
            synchronized (relationMap) {
                RelationOperator operator = relationMap.get(pk);
                if (null == operator) {
                    operator = new RelationOperator(initOrderNum);
                    relationMap.put(pk, operator);
                }
                operator.offset = fId_off;  //change the object, doesn't affect map
                curRecNum++;
            }

        } else {

            if (isReady) {
                synchronized (relationMap) {
                    RelationOperator operator = relationMap.get(pk);
                    if (null == operator) {
                        operator = new RelationOperator(initOrderNum);
                        relationMap.put(pk, operator);
                    }
                    operator.offset = fId_off;  //change the object, doesn't affect map
                    curRecNum++;
                }
                isReady = false;

            } else {

                //dump data out, copy first for efficency
                synchronized (relationMap) {
                    if (!isReady && (capacity - curRecNum) <= THRESHOD) {

                        //time-consuming, but only couple of times, sorting in a sequential writing order
                        final TreeSet<RelationOperator> mirrorSet = new TreeSet<RelationOperator>(relationMap.values());
                        curRecNum = 0;
                        isReady = true; //allow insert

                        new Thread() {
                            @Override
                            public void run() {

                                System.out.println("--Dump set size: " + mirrorSet.size()+"  , "+mirrorSet);

                                int startPos = mirrorSet.first().indexOff;  //seek first index

                                boolean appendOnly = false;
                                if (startPos == Integer.MAX_VALUE) {
                                    startPos = tailPos;
                                    appendOnly = true;
                                }

                                File file = new File(storePath);
                                RandomWriter randomWriter = new RandomWriter(file, startPos, buffSize);

                                for (RelationOperator operator : mirrorSet) {

                                    if (operator.indexOff == Integer.MAX_VALUE) {
                                        appendOnly = true;
                                    }
                                    if (appendOnly) {
                                        //byte[] key = new byte[21];  //good 20, buyer 20-21
                                        //String pk = operator.pk;
                                        //System.arraycopy(pk.getBytes(), 0, key, 0, pk.length());
                                        //appender.append(key);

                                        boolean isWritten = false;
                                        synchronized (operator.orderList) {

                                            if (operator.orderList.size() > 0) {
                                                isWritten = true;
                                                for (Long orderLoc : operator.orderList) {
                                                    randomWriter.writeLong(orderLoc);
                                                }
                                                operator.writtenNum = operator.orderList.size();
                                                operator.orderList.clear();   //clear list
                                                operator.indexOff = tailPos;    //last end of file
                                            }
                                            //no data, won't update
                                        }

                                        if (isWritten) {
                                            //write blank
                                            int total = (type == FileParser.BUYER) ? FileParser.BUYER_ORDER_NUM : FileParser.GOOD_ORDER_NUM;
                                            int blankNum = total - operator.writtenNum;//not concurrent modified in a short time
                                            byte[] blankBytes = new byte[blankNum * 8];
                                            randomWriter.write(blankBytes); //manually put blank to guarantee correct cursor
                                            tailPos += total * 8;
                                        }
                                        continue;   //no need to run
                                    }

                                    //has written before
                                    synchronized (operator.orderList) {
                                        //change since last flush
                                        if (operator.orderList.size() > 0) {

                                            int start = operator.indexOff + operator.writtenNum * 8;

                                            int cnt = 1;
                                            randomWriter.writeExactLong(start, operator.orderList.get(0));
                                            for (; cnt < operator.orderList.size(); cnt++) {
                                                randomWriter.writeLong(operator.orderList.get(cnt));
                                                cnt++;
                                            }
                                            operator.writtenNum += cnt;  //all flushed
                                            operator.orderList.clear();

                                        }
                                        //didn't change file length as written in the middle of file

                                    }

                                }
                                randomWriter.close();
                                System.out.println(":  Good/Buyer Dump Thread: " + storePath + ", Position: " + tailPos);
                                System.out.println("MEMORY FREE: " + Runtime.getRuntime().freeMemory());
                            }

                        }.start();
                    }

                    RelationOperator operator = relationMap.get(pk);
                    if (null == operator) {
                        operator = new RelationOperator(initOrderNum);
                        relationMap.put(pk, operator);
                    }
                    operator.offset = fId_off;  //put the last kid
                    curRecNum++;
                }
                isReady = false;

            }
        }
    }

        /**
         * load exactly this good or buyer's order list
         *
         * @param pk
         * @return
         */
        public List<Long> loadOrderLocs (String pk){
            RelationOperator operator = relationMap.get(pk);
            List<Long> locList = operator.orderList;

            //some orders are in disk
            if (operator.writtenNum > 0) {
                //can use bytebufferOperator instead
                FastReader reader = new FastReader(new File(storePath), operator.indexOff, operator.writtenNum * 8);//8byte
                long n;
                while ((n = reader.getLong()) > 0)
                    locList.add(n);
                reader.close();
            }
            return locList;
        }

        /**
         * if the big one contains the small one, return true
         *
         * @param smallOne
         * @param bigOne
         * @return
         */

    public static boolean roughlyEqual(byte smallOne[], byte bigOne[]) {
        for (int i = 0; i < smallOne.length; i++)
            if (smallOne[i] != bigOne[i])
                return false;

        return true;
    }

    public static void main(String args[]) {
        String buyerId = "tp-ad6c-df9435e98ab8";
        String goodId = "aye-a80a-32ff1d143328";

        System.out.println("buyerID size: " + buyerId.length() + ", to bytes: " + buyerId.getBytes().length);
        System.out.println("goodID size: " + goodId.length() + ", to bytes: " + goodId.getBytes().length);

        byte[] bytes = new byte[21];
        System.arraycopy(buyerId.getBytes(), 0, bytes, 0, buyerId.length());
        System.out.println("Is equal: " + roughlyEqual(buyerId.getBytes(), bytes));
    }

}
