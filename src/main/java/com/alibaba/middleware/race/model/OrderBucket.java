package com.alibaba.middleware.race.model;

import com.alibaba.middleware.race.file.FileParser;
import com.alibaba.middleware.race.util.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Created by zhaoz on 2016/7/30.
 * Matain map <orderId, fileID_offset>
 * 1. in parsing phrase, flush offset info to disk for limited memory
 * 2. after parsing, load file to memory, construct map selectively
 */
public class OrderBucket {

    private int buffSize;
    private int capacity;                       //number of record in each bucket
    private int maxBucketSize;
    private int writingPosition = 0;
    //private long minId;
    private int bucketId;
    private int curRecNum = 0;           //current record number, observable by all thread
    private volatile boolean isReady = false;   //whether the new bucket is ready for insertion
    private int totalNum=0;
    //public TreeMap<Long, Long> offsetMap;           //<orderId, fileID_offset>,object is enough
    public List<IndexPair> offsetList;
    public ConcurrentHashMap<Long, Long> overFlowMap;        //adopt overflow data & last not dump data
    private String storePath;
    private byte THRESHOD = 0;                 //error allow range
    private List<Integer> slicePositions;

    private int hashSeed = sun.misc.Hashing.randomHashSeed(this);

    public OrderBucket(int bucketId, String storePath, int buffSize, int capacity) {
        this.bucketId = bucketId;
        this.storePath = storePath;
        this.buffSize = buffSize;
        this.capacity = capacity;
        this.maxBucketSize = (int) (capacity * 1.3); //bucketSize应当略大于能存放的条目数. 乘以一个参数,该参数可调整.

        this.overFlowMap = new ConcurrentHashMap<Long, Long>(1024);  //overflow cache
        this.offsetList = new ArrayList<IndexPair>(maxBucketSize);
        this.slicePositions = new ArrayList<Integer>();
        //init
        for (int i = 0; i < maxBucketSize; i++) {
            offsetList.add(null);
        }

        System.out.println(storePath+ " , last slice: "+FileParser.LAST_SLICE);
       // this.minId=FileParser.MIN + FileParser.SIZE * bucketId;
       // System.out.println("bucket " + bucketId + " range: [" + minId + ", " + (minId + FileParser.SIZE) + ")");
    }

    public void put(long orderId, byte fileId, long offset) {

        long fId_off = BitHelper.assembleOrderFidOff(fileId, offset);

        //last slice store in memory (1 slice * 60 about 3G in memory)
        /*if (slicePositions.size() >= FileParser.LAST_SLICE) {
            overFlowMap.put(orderId, fId_off);
            return;
        }*/
        if ((capacity - curRecNum) > THRESHOD) {
            synchronized (this) {
                putItem(new IndexPair(orderId, fId_off));
                curRecNum++;
            }

        } else {

            if (isReady) {
                //after new bucket created
                synchronized (this) {
                    putItem(new IndexPair(orderId, fId_off));
                    curRecNum++;
                }
                isReady = false;    //if the new bucket is ready for inserting, back to normal state
            } else {

                //only one thread reinitialize the data list
                synchronized (this) {

                    //while the bucket is not created & bucket is full, this thread create a new bucket
                    if (((capacity - curRecNum) <= THRESHOD) && !isReady) {
                        final List<IndexPair> persistList = offsetList; //a copy of old data
                        synchronized (offsetList) {
                            offsetList = new ArrayList<IndexPair>(maxBucketSize);//a new bucket created, allow elements putting
                            //init anyway
                            for (int i = 0; i < maxBucketSize; i++) {
                                offsetList.add(null);
                            }
                        }

                        totalNum += curRecNum;
                        curRecNum = 0;
                        isReady = true;

                        //write thread dumps the data in bucket & record their position in output file
//                        new Thread() {
//                            @Override
//                            public void run() {

                                slicePositions.add(writingPosition);    //before flushing, record starting point
                                FastAppender appender = new FastAppender(new File(storePath), writingPosition, buffSize);
//                                try {
//                                    RandomAccessFile randomAccessFile = new RandomAccessFile(new File(storePath), "rw");
//                                     randomAccessFile.seek(writingPosition);

                                int cnt=0;
                                System.out.println(storePath+" , slice "+slicePositions.size());
                                for (IndexPair pair : persistList) {
                                    if (pair != null) {
                                        if(pair.K == 3008479){
                                            System.out.println(pair);
                                        }
                                        long K = pair.K | (pair.confNum << 40); //not likely > 2^24
                                        appender.appendLong(K);
                                        appender.appendLong(pair.V);
                                        //randomAccessFile.writeLong(K);
                                        //randomAccessFile.writeLong(pair.V);
                                    } else {
                                        appender.appendLong(-2); //useless
                                        appender.appendLong(-2);
//                                        randomAccessFile.writeLong(-2);
//                                        randomAccessFile.writeLong(-2);
                                    }
                                    writingPosition += 16;
                                }

//                                    randomAccessFile.close();
//                                } catch (FileNotFoundException e) {
//                                    e.printStackTrace();
//                                } catch (IOException e) {
//                                    e.printStackTrace();
//                                }
                                System.out.println(System.nanoTime() + ":  Bucket Dump Thread: " + storePath + ", Position: "
                                        + writingPosition + " , dump size: " + persistList.size());

                                appender.close();
                                persistList.clear();

                                System.out.println("MEMORY FREE: " + Runtime.getRuntime().freeMemory());
                            }
                 //       }.start();

                 //   }
                }


                //while new map is not ready, spin
                while (!isReady && (capacity - curRecNum) <= THRESHOD) ;
                //System.out.println( System.nanoTime()+" Thread "+ Thread.currentThread().getName()+" spin " );   //spin

                //put record to the new bucket
                synchronized (this) {
                    putItem(new IndexPair(orderId, fId_off));
                    curRecNum++;
                }
                isReady = false;    //back to normal insertion
            }
        }
    }

    /**
     * Just let them in memory, about 1/3, as cache
     * writing the remaining data to file
     * build index (location info) & record relation in the meantime
     */
    public void cleanUp() {

        if (curRecNum != 0) {
            //FastAppender appender = new FastAppender(new File(storePath), writingPosition, buffSize);
            for (IndexPair pair : offsetList) {
                if (pair != null) {
                    overFlowMap.put(pair.K, pair.V);
                    //appender.appendLong(pair.K);
                    //appender.appendLong(pair.V);
                } /*else {
                    appender.appendLong(0); //useless
                    appender.appendLong(0);
                }*/

                writingPosition += 16;
            }
            offsetList.clear();
            //appender.close();
        }

        System.out.println("---- Bucket " + storePath + "  Cleaning , Position: " + writingPosition+", written total: "+totalNum);

    }

    /**
     * 将indexPair放到offsetList的相应位置
     *
     * @param ip
     */
    private void putItem(IndexPair ip) {

        int offset = getOffset(ip.K);
        IndexPair oriIp;// = offsetList.get(offset);
        boolean isOverFlow = false;
        //every item want to seat here is a confliction
        synchronized (offsetList) {
            while ((oriIp = offsetList.get(offset)) != null) {
                offset++;                       //如果该位置有对象，则往后延
                // 增加origin IndexPair中冲突计数
                oriIp.confNum = oriIp.confNum + 1;

                if (offset >= maxBucketSize) {
                    //offset -= maxBucketSize; //往前滚不利于查找
                    System.out.println("Over flow Size: " + overFlowMap.size() + " at " + storePath);
                    isOverFlow = true;
                    break;
                }
            }

            if (!isOverFlow) {
                offsetList.set(offset, ip);
            }
        }
        if(isOverFlow){
            //溢出的数据直接放内存
            overFlowMap.put(ip.K, ip.V);
        }
    }

    public static int DJBHash(String str)
    {
        int hash = 5381;

        for(int i = 0; i < str.length(); i++)
        {
            hash = ((hash << 5) + hash) + str.charAt(i);
        }

        return (hash & 0x7FFFFFFF);
    }

    /**
     * 计算应当放到offsetList的哪个位置, 不考虑冲突
     *
     * @param orderId
     * @return
     */
    private int getOffset(Long orderId) {
        // 现在使用最粗暴的方式, 取模
        //return (int) (Math.abs(orderId.hashCode() ^ hashSeed) % maxBucketSize);
        //利用数据的特征，稀松，一定程度上等距, 第一个桶例外
       //return (int)((orderId - minId) / STEP)  % maxBucketSize;
        return DJBHash(orderId.toString()) % maxBucketSize;
    }

    private static final int QUERY_SIZE = 3000;

    /**
     * load exactly this order's fileId_offset
     *
     * @param orderId
     * @return
     */
    public long loadIndex(long orderId) {
        //外面已经判断不在内存中才会使用这个函数
        Long n;
if((n = overFlowMap.get(orderId)) != null){
    System.out.println(" MEMORY HIT INDEX");
    return n;
}

        /*ByteBufferOperator breader;
        File file = new File(this.storePath);
        int bsize = (int)file.length();
        breader = new ByteBufferOperator(new File(this.storePath), 0L, (int)(new File(this.storePath).length()));

        for (int r = 0; r < bsize; r ++) {
            long readId = breader.readLong();
            if ((readId & 0x000000FFFFFFFFFFL) == orderId) {
                // 找到了
                System.out.println("Seek success, offset = " + r);
                return breader.readLong();
            }
            breader.readLong();
        }

        System.out.println("Seek fail");

        return -1;*/

        // 首先load  QUERY_SIZE个条目出来. 如果越界了还没找到, 再load冲突数个条目出来
        File file = new File(storePath);

        long start = getOffset(orderId) * 16; //logic pos -> absolute pos
        ByteBufferOperator reader = new ByteBufferOperator(file, start, QUERY_SIZE * 16);

        for (int i = 0; i < slicePositions.size(); i++) {
            System.out.println("Seek position in slice "+i+" , pos: " + slicePositions.get(i));

            if(i > 0) {
                reader.seek(start + slicePositions.get(i));
            }

            long readId = reader.readLong();
            if ((readId & 0x000000FFFFFFFFFFL) == orderId) {
                // 找到了

                System.out.println("Seek succeed " );
                return reader.readLong();

            }
            else if(readId < 0){
                System.out.println("!! : "+readId+", orderId "+orderId+"not in slice "+i);
                continue;
            }
            int confNum = (int) (readId >>> 40);
            reader.readLong();  //pass 1
            start +=8;
            System.out.println("-conflict num: "+confNum);
            //read longs
            if(confNum == 0){
                System.out.println(" No conflicts in slice "+slicePositions.get(i));
                continue;

            }else {

                int cnt = 1; //record has been read
                while(true){

                    readId = reader.readLong();
                    if ((readId & 0x000000FFFFFFFFFFL) == orderId) {
                        //find
                        System.out.println("Seek success in: " + start);
                        return reader.readLong();
                    }

                    //blank data or no conflicts at current data, no need to continue
                    if (readId < 0 || (readId >>> 40) == 0) {
                        break;
                    }

                    //move on until no conflict occurs
                    reader.readLong();
                    cnt++;
                    if(cnt == QUERY_SIZE){
                        reader.seek(slicePositions.get(i) + QUERY_SIZE *16);
                    }
                    System.out.println("--keep looking at " + start);
                }
            }
        }

        reader.close();
        System.out.println("!!Not found orderId, orderId=" + orderId);
        return -1;

    }

    /**
     * load index from disk with size num
     *
     * @param num
     * @return
     */
    public Map<Long, Long> loadIndices(int num) {
        return new HashMap<Long, Long>();
    }

    public Map<Long, Long> loadAll() {
        return new HashMap<Long, Long>();
    }


    public String getStorePath() {
        return this.storePath;
    }

    public Map<Long, Long> getOverFlowMap() {
        return this.overFlowMap;
    }

    static class TestThread implements Runnable {
        private OrderBucket bucket;

        public TestThread(OrderBucket bucket) {
            this.bucket = bucket;
        }

        @Override
        public void run() {

            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            int cnt = 0;
            while (cnt++ < 5) {
                bucket.put(Thread.currentThread().getId() * 5 + cnt, (byte) 11, (long) Integer.MAX_VALUE + cnt);
                System.out.println(System.nanoTime() + " Thread " + Thread.currentThread().getName() + " put " + cnt);
            }
        }
    }

    public static void main(String args[]) throws InterruptedException {

        //multi-thread testing
        OrderBucket bucket = new OrderBucket(0,"bucket", 128, 15);
        Thread t1 = new Thread(new TestThread(bucket));
        Thread t2 = new Thread(new TestThread(bucket));
        Thread t3 = new Thread(new TestThread(bucket));
        Thread t4 = new Thread(new TestThread(bucket));
        Thread t5 = new Thread(new TestThread(bucket));
        Thread t6 = new Thread(new TestThread(bucket));
        Thread t7 = new Thread(new TestThread(bucket));
        Thread t8 = new Thread(new TestThread(bucket));
        Thread t9 = new Thread(new TestThread(bucket));

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        t5.start();
        t6.start();
        t7.start();
        t8.start();
        t9.start();
        Thread.sleep(1000);

        for (Map.Entry<Long, Long> entry : bucket.overFlowMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue() + " File ID: "
                    + BitHelper.getOrderFileId(entry.getValue()) +
                    ", Offset: " + BitHelper.getOrderOffset(entry.getValue()));
        }
        System.out.println("overflow map size: " + bucket.overFlowMap.size());

        //read data
        //File file = new File("bucket");
        //FastReader reader = new FastReader(file,0,file.length());
    }
}
