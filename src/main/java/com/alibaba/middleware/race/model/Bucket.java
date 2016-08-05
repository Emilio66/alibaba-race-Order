package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.file.FileParser;
import com.alibaba.middleware.race.util.FastAppender;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * similar data saved in the same bucket
 * bucket will be flush to disk while it's full
 * at this point, data can't be written until a new bucket is created
 */
public class Bucket {
    private int id;
    private int buffSize;   //every time we append a new record
    private int capacity;   //number of record in each bucket
    private int writingPosition;

    private volatile int curRecNum;             //current record number, observable by all thread
    private volatile boolean isReady = false;   //whether the new bucket is ready for insertion

    private String storePath;
    private List<Record> list; // type of record will be passed from parameters
    public Map<Comparable, Integer> offsetMap;  //maybe exist in memory
    private byte type;

    private byte THRESHOD = 10;

    public Bucket(String storePath, int buffSize, int capacity, int id, byte type) {
        this.type = type;
        this.id = id;
        this.buffSize = buffSize;
        this.storePath = storePath;
        this.capacity = capacity;
        this.list = new CopyOnWriteArrayList<Record>();
        //only order needs offset map
        //if(type == FileParser.ORDER)
        //    this.offsetMap = new HashMap<Comparable, Integer>(2048);
        if (type != FileParser.ORDER)
            this.offsetMap = new HashMap<Comparable, Integer>(2048);//test buyer & good
        this.curRecNum = 0;
        this.writingPosition = 0;
    }

    //almost full, no need to be accurate, allow some concurrent put
    public boolean isFull() {
        return (capacity - curRecNum) < THRESHOD;
    }

    /**
     * put a record into a bucket
     * if the bucket is full, new a bucket, flush the old bucket to disk & make it thread safe
     * build index (location info) & record relation in the meantime
     *
     * @param record
     */
    public void put(Record record) {
        final SimpleDateFormat time=new SimpleDateFormat("HH:mm:ss");
        if ( (capacity - curRecNum) > THRESHOD) {
            synchronized (list) {
                list.add(record);
                curRecNum++;
            }

        } else {
            //System.out.println(  System.nanoTime()+" Thread " + Thread.currentThread().getName() + " come");

            if (isReady) {
                //System.out.println(" Thread "+ Thread.currentThread().getName()+"== is ready -- curRec: "+curRecNum);
                //after new bucket created
                synchronized (list) {
                    list.add(record);
                    curRecNum++;
                }
                isReady = false;    //if the new bucket is ready for inserting, back to normal state
            } else {

                if (!isReady) {
                    //only one thread reinitialize the data list
                    synchronized (this) {
                       //System.out.println(  System.nanoTime()+" Thread "+ Thread.currentThread().getName()+" intent to CREATE BUCKET: "
                       //         + storePath+" , curRec: "+curRecNum);

                        //while the bucket is not created & bucket is full, this thread create a new bucket
                        if ( ((capacity - curRecNum) <= THRESHOD) && !isReady) {
                            final List<Record> finalOutList = list; //a copy of old data
                            list = new CopyOnWriteArrayList<Record>();//a new bucket created, allow elements putting
                            curRecNum = 0;
                            isReady = true;
                            System.out.println(  System.nanoTime() +" Thread "+ Thread.currentThread().getName()+" NEW BUCKET !! "
                                    + storePath+" , curRec: "+curRecNum);
                            //write thread dumps the data in bucket & record their position in output file
                            new Thread() {
                                @Override
                                public void run() {

                                    FastAppender appender = new FastAppender(new File(storePath),
                                            writingPosition, buffSize);
                                    if (FileParser.ORDER == type) {
                                        for (Record entry : finalOutList) {
                                            byte[] data = entry.value;
                                            appender.append(data);
                                            //offsetMap.put(entry.key, writingPosition);  //add offset of this record

                                            //building index for buyer & good
//                                         long file_offset = id;
//                                         file_offset = (file_offset << 32) | writingPosition;
//
//                                            String line = new String(data);
//                                            String buyerId = FileParser.retrievePK(line, FileParser.BUYER);
//                                            String goodId = FileParser.retrievePK(line, FileParser.GOOD);
//
//                                            int index = Math.abs(buyerId.hashCode()) % FileParser.BUYER_MAP_NUM;
//                                            Map<String, RelationOperator> buyerOrderMap = FileParser.buyerOrderRelationMaps.get(index);
//                                            RelationOperator buyerOperator;
//                                            if(null == (buyerOperator = buyerOrderMap.get(buyerId))) {
//                                                buyerOperator = new RelationOperator(FileParser.BUEYR_ORDER_NUM);
//                                                buyerOrderMap.put(buyerId, buyerOperator);
//                                            }
//
//
//                                            buyerOperator.add(file_offset);
//                                            //buyerOrderMap.put(buyerId, buyerOrders); //not necessary, data has changed
//
//                                            index = Math.abs(goodId.hashCode()) % FileParser.GOOD_MAP_NUM;
//                                            Map<String, RelationOperator> goodOrderMap = FileParser.goodOrderRelationMaps.get(index);
//                                            RelationOperator goodOperator;
//                                            if(null == (goodOperator = goodOrderMap.get(goodId))) {
//                                                goodOperator = new RelationOperator(FileParser.GOOD_ORDER_NUM);
//                                                goodOrderMap.put(goodId, goodOperator);
//                                            }
//
//                                            goodOperator.add(file_offset);
                                            //goodOrderMap.put(goodId, goodOrders);

                                            writingPosition += data.length;
                                        }
                                    } else {
                                        for (Record entry : finalOutList) {
                                            byte[] data = entry.value;
                                            appender.append(data);
                                            offsetMap.put(entry.key, writingPosition);  //offset saved in relation object
//                                            if(FileParser.GOOD == type){
//                                                int index = Math.abs(entry.key.hashCode()) % FileParser.GOOD_MAP_NUM;
//                                                Map<String, RelationOperator> map= FileParser.goodOrderRelationMaps.get(index);
//                                                RelationOperator operator;
//                                                if((operator = map.get(entry.key)) == null) {
//                                                    operator = new RelationOperator(FileParser.GOOD_ORDER_NUM);
//                                                    map.put(entry.key.toString(), operator);
//                                                }
//                                                operator.setOffset(writingPosition);
//
//                                            }else{
//                                                int index = Math.abs(entry.key.hashCode()) % FileParser.BUYER_MAP_NUM;
//                                                Map<String, RelationOperator> map= FileParser.buyerOrderRelationMaps.get(index);
//                                                RelationOperator operator;
//                                                if((operator = map.get(entry.key)) == null) {
//                                                    operator = new RelationOperator(FileParser.BUEYR_ORDER_NUM);
//                                                    map.put(entry.key.toString(), operator);
//                                                }
//                                                operator.setOffset(writingPosition);
//
//                                            }

                                            writingPosition += data.length;
                                        }
                                    }
                                    appender.close();
                                    System.out.println( System.nanoTime() + ":  Bucket Dump Thread: " + storePath + ", Position: "
                                            + writingPosition+ " , size: " + finalOutList.size());
                                    finalOutList.clear();   //release space

                                    System.out.println("MEMORY FREE: "+Runtime.getRuntime().freeMemory());
                                }
                            }.start();

                        }
                    }

                }

                //while new list is not ready, spin
                while(!isReady && (capacity - curRecNum) <= THRESHOD);
                    //System.out.println( System.nanoTime()+" Thread "+ Thread.currentThread().getName()+" spin " );   //spin

                //put record to the new bucket
                synchronized (list) {
                    list.add(record);
                    curRecNum++;
                    //System.out.println( time.format(new Date())+" Thread "+ Thread.currentThread().getName()+
                    //        "--full put curRec: "+curRecNum+", size: "+list.size());
                }
                isReady = false;    //back to normal insertion
            }
        }
    }

    /**
     * writing the remaining data to file
     * build index (location info) & record relation in the meantime
     */
    public void cleanUp() {
        //      try {
        if (curRecNum != 0) {
            FastAppender appender = new FastAppender(new File(storePath), writingPosition, buffSize);
            if (FileParser.ORDER == type) {
                for (Record entry : list) {
                    byte[] data = entry.value;
                    appender.append(data);
                    //offsetMap.put(entry.key, writingPosition);  //add offset of this record

                    //building index for buyer & good
//                        long file_offset = id;
//                        file_offset = (file_offset << 32) | writingPosition;
//
//                        String line = new String(data);
//                        String buyerId = FileParser.retrievePK(line, FileParser.BUYER);
//                        String goodId = FileParser.retrievePK(line, FileParser.GOOD);
//
//                        int index = Math.abs(buyerId.hashCode()) % FileParser.BUYER_MAP_NUM;
//                        Map<String, RelationOperator> buyerOrderMap = FileParser.buyerOrderRelationMaps.get(index);
//                        RelationOperator buyerOperator;
//                        if(null == (buyerOperator = buyerOrderMap.get(buyerId))) {
//                            buyerOperator = new RelationOperator(FileParser.BUEYR_ORDER_NUM);
//                            buyerOrderMap.put(buyerId, buyerOperator);
//                        }
//
//
//                        buyerOperator.add(file_offset);
//                        //buyerOrderMap.put(buyerId, buyerOrders); //not necessary, data has changed
//
//                        index = Math.abs(goodId.hashCode()) % FileParser.GOOD_MAP_NUM;
//                        Map<String, RelationOperator> goodOrderMap = FileParser.goodOrderRelationMaps.get(index);
//                        RelationOperator goodOperator;
//                        if(null == (goodOperator = goodOrderMap.get(goodId))) {
//                            goodOperator = new RelationOperator(FileParser.GOOD_ORDER_NUM);
//                            goodOrderMap.put(goodId, goodOperator);
//                        }
//
//                        goodOperator.add(file_offset);
                    //goodOrderMap.put(goodId, goodOrders);

                    writingPosition += data.length;
                }
            } else {
                for (Record entry : list) {
                    byte[] data = entry.value;
                    appender.append(data);
                    offsetMap.put(entry.key, writingPosition);  //offset saved in relation object
//                        if(FileParser.GOOD == type){
//                            int index = Math.abs(entry.key.hashCode()) % FileParser.GOOD_MAP_NUM;
//                            Map<String, RelationOperator> map= FileParser.goodOrderRelationMaps.get(index);
//                            RelationOperator operator;
//                            if((operator = map.get(entry.key)) == null) {
//                                operator = new RelationOperator(FileParser.GOOD_ORDER_NUM);
//                                map.put(entry.key.toString(), operator);
//                            }
//                            operator.setOffset(writingPosition);
//
//                        }else{
//                            int index = Math.abs(entry.key.hashCode()) % FileParser.BUYER_MAP_NUM;
//                            Map<String, RelationOperator> map= FileParser.buyerOrderRelationMaps.get(index);
//                            RelationOperator operator;
//                            if((operator = map.get(entry.key)) == null) {
//                                operator = new RelationOperator(FileParser.BUEYR_ORDER_NUM);
//                                map.put(entry.key.toString(), operator);
//                            }
//                            operator.setOffset(writingPosition);
//
//                        }

                    writingPosition += data.length;
                }
            }
            appender.close();

        }

        list.clear(); //release some space
        System.out.println("---- Bucket " + storePath + "  Cleaning , Position: " + writingPosition);

    }
//no need to write index
    //writing index as a object, index file name is the same as data, different in suffix
//            String indexPath = storePath + ".idx";
//            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(indexPath));
//            objectOutputStream.writeObject(offsetMap);  //write object to disk
//            objectOutputStream.close();

//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }

    static class TestThread implements Runnable{
        private Bucket bucket;
        public TestThread(Bucket bucket){
            this.bucket = bucket;
        }
        @Override
        public void run(){

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int cnt =0;
            while(cnt ++ < 5){
                bucket.put(new Record());
                System.out.println(System.nanoTime()+" Thread "+ Thread.currentThread().getName()+" put 1");
            }
        }
    }
    public static void main(String args[]) throws InterruptedException {
        long start = System.nanoTime();
        List<Record> list = new ArrayList<Record>(500000);
        long end = System.nanoTime();
        System.out.println("New A 500k List takes ns: "+(end - start));
        //multi-thread testing
        Bucket bucket = new Bucket("bucket.txt", 128, 5, 1,FileParser.ORDER);
        Thread t1 = new Thread(new TestThread(bucket));
        Thread t2 = new Thread(new TestThread(bucket));
        Thread t3 = new Thread(new TestThread(bucket));
        Thread t4 = new Thread(new TestThread(bucket));
        Thread t5 = new Thread(new TestThread(bucket));
        Thread t6 = new Thread(new TestThread(bucket));

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        Thread.sleep(1000);
        System.out.println("Finally list size: "+bucket.list.size());
    }

}
