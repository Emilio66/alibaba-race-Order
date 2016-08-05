package com.alibaba.middleware.race;

import com.alibaba.middleware.race.file.FileParser;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.util.BitHelper;
import com.alibaba.middleware.race.util.FastReader;

import java.io.*;
import java.nio.file.FileStore;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.CountDownLatch;

/**
 * Created by zhaoz on 2016/7/16.
 * Entrance of race
 */
public class OrderSystemImpl implements OrderSystem {
    public static volatile int orderQueryCnt = 0;
    public static int buyerQueryCnt = 0;
    public static int salerQueryCnt = 0;
    public static int sumQueryCnt = 0;
    public static String orderStorePath, goodStorePath, buyerStorePath;

    public static final String disk1 = "/disk1";
    public static final String disk2 = "/disk2";
    public static final String disk3 = "/disk3";

    //private MappedByteBuffer byteBuffer;

    public OrderSystemImpl() {
        new FileParser();   //pre-load FileParser's static data
    }

    public static HashSet<String> createQueryKeys(Collection<String> keys) {
        if (keys == null) {
            return null;
        }
        return new HashSet<String>(keys);
    }

    @Override
    public void construct(Collection<String> orderFiles, Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {

        long start = System.currentTimeMillis();
        System.out.println("------------------------SYSTEM INFO --------------------------");
        List<List<String>> disk1FileList = new ArrayList<List<String>>(3);
        List<List<String>> disk2FileList = new ArrayList<List<String>>(3);
        List<List<String>> disk3FileList = new ArrayList<List<String>>(3);
        //init
        for (int i = 0; i < 3; i++) {
            disk1FileList.add(new ArrayList<String>());
            disk2FileList.add(new ArrayList<String>());
            disk3FileList.add(new ArrayList<String>());
        }

        System.out.println("Order File:  ");
        byte orderFN = 0;
        long totalSize = 0;

        for (String fileName : orderFiles) {
            if (fileName.contains(disk1)) {
                disk1FileList.get(FileParser.ORDER).add(fileName);
                FileParser.orderFIDMap.put(orderFN, fileName);     //add file id

            } else if (fileName.contains(disk2)) {
                disk2FileList.get(FileParser.ORDER).add(fileName);
                FileParser.orderFIDMap.put(orderFN, fileName);

            } else if (fileName.contains(disk3)) {
                disk3FileList.get(FileParser.ORDER).add(fileName);
                FileParser.orderFIDMap.put(orderFN, fileName);

            } else {
                System.out.println("\n\n !!!!!!!!!!!!!!!------- WTF: " + fileName);

            }
            File file = new File(fileName);
            long length = file.length();
            System.out.println(file.getAbsolutePath() + " size: " + length);
            orderFN++;
            totalSize += length;
        }
        System.out.println("Total file number: " + orderFN + ", size: " + (totalSize * 1.0 / (1L << 30)) + " GB "
                + ", FID MAP SIZE: " + FileParser.orderFIDMap.size());

        System.out.println("Buyer File:  ");
        byte buyerFN = 0;
        totalSize = 0;

        for (String fileName : buyerFiles) {
            if (fileName.contains(disk1)) {
                disk1FileList.get(FileParser.BUYER).add(fileName);
                FileParser.buyerFIDMap.put(buyerFN, fileName);

            } else if (fileName.contains(disk2)) {
                disk2FileList.get(FileParser.BUYER).add(fileName);
                FileParser.buyerFIDMap.put(buyerFN, fileName);

            } else if (fileName.contains(disk3)) {
                disk3FileList.get(FileParser.BUYER).add(fileName);
                FileParser.buyerFIDMap.put(buyerFN, fileName);

            } else {
                System.out.println("\n\n!!!!!!!!!!!!!!!---- WTF: " + fileName);

            }
            File file = new File(fileName);
            long length = file.length();
            System.out.println(file.getAbsolutePath() + " size: " + length);
            buyerFN++;
            totalSize += length;
        }
        System.out.println("Total file number: " + buyerFN + ", size: " + (totalSize * 1.0 / (1L << 30)) + " GB "
                + ", FID MAP SIZE: " + FileParser.buyerFIDMap.size());

        System.out.println("Good File:  ");
        byte goodFN = 0;
        totalSize = 0;
        for (String fileName : goodFiles) {
            if (fileName.contains(disk1)) {
                disk1FileList.get(FileParser.GOOD).add(fileName);
                FileParser.goodFIDMap.put(goodFN, fileName);

            } else if (fileName.contains(disk2)) {
                disk2FileList.get(FileParser.GOOD).add(fileName);
                FileParser.goodFIDMap.put(goodFN, fileName);

            } else if (fileName.contains(disk3)) {
                disk3FileList.get(FileParser.GOOD).add(fileName);
                FileParser.goodFIDMap.put(goodFN, fileName);

            } else {
                System.out.println("--- WTF: " + fileName);

            }
            File file = new File(fileName);
            long length = file.length();
            System.out.println(file.getAbsolutePath() + " size: " + length);
            goodFN++;
            totalSize += length;
        }
        System.out.println("Total file number: " + goodFN + ", size: " + (totalSize * 1.0 / (1L << 30)) + " GB "
                + ", FID MAP SIZE: " + FileParser.goodFIDMap.size());

        System.out.println("Store path:  ");
        File file = null;

        for (String fileName : storeFolders) {
            file = new File(fileName);
            System.out.println(file.getAbsolutePath() + " size: " + file.length());
            File[] files = file.listFiles();
            for (File f : files) {
                System.out.println("subpath: " + f.getAbsolutePath() + " size: " + f.length());
            }
        }
        System.out.println("");

        Path path = FileSystems.getDefault().getPath("./");
        FileStore store = Files.getFileStore(path);
        System.out.println("Store name: ' " + store.name() + " ' ; type : " + store.type());
        System.out.println("Total space in VOLUME: " + store.getTotalSpace());
        System.out.println("Free space in VOLUME: " + store.getUsableSpace());
        final Runtime runtime = Runtime.getRuntime();
        System.out.println("Max memory: " + runtime.maxMemory());
        System.out.println("Total memory: " + runtime.totalMemory());
        System.out.println("Free memory: " + runtime.freeMemory());
        System.out.println("Available processors: " + runtime.availableProcessors());

        //String orderStorePath, goodStorePath, buyerStorePath;
        if (storeFolders.size() >= 3) {
            String[] folds = new String[3];
            storeFolders.toArray(folds);
            orderStorePath = folds[1];  //writing on the second disk
            goodStorePath = orderStorePath;
            buyerStorePath = orderStorePath;

        } else if (storeFolders.size() == 2) {
            String[] folds = new String[2];
            storeFolders.toArray(folds);
            orderStorePath = folds[0];
            goodStorePath = folds[1];
            buyerStorePath = folds[1];
        } else {
            String[] folds = new String[1];
            storeFolders.toArray(folds);
            orderStorePath = folds[0];
            goodStorePath = folds[0];
            buyerStorePath = folds[0];
        }

        System.out.println("-------------------------- Parsing --------------------------");

        //错开读，同时读不同的disk相近区域，减少寻道时间，利用多块磁盘的吞吐量及多核的优势
        final Collection<String> orderReadList = new ArrayList<String>(43);
        final Collection<String> buyerReadList = new ArrayList<String>(5);
        final Collection<String> goodReadList = new ArrayList<String>(5);

        orderReadList.addAll(disk3FileList.get(FileParser.ORDER));//order: disk3,1,2
        orderReadList.addAll(disk1FileList.get(FileParser.ORDER));
        orderReadList.addAll(disk2FileList.get(FileParser.ORDER));

        buyerReadList.addAll(disk2FileList.get(FileParser.BUYER));//buyer: disk 2,1,3
        buyerReadList.addAll(disk1FileList.get(FileParser.BUYER));
        buyerReadList.addAll(disk3FileList.get(FileParser.BUYER));

        goodReadList.addAll(disk1FileList.get(FileParser.GOOD)); //good: disk 1,2,3
        goodReadList.addAll(disk2FileList.get(FileParser.GOOD));
        goodReadList.addAll(disk3FileList.get(FileParser.GOOD));

        //init buckets
        FileParser.init(orderStorePath, goodStorePath, buyerStorePath);

        //multi-thread running

        final CountDownLatch latch = new CountDownLatch(3);//start to parsing 3 disk
        new Thread() {
            @Override
            public void run() {
                super.run();
                FileParser.parseBuyerFiles(buyerReadList);
                latch.countDown();
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                super.run();
                FileParser.parseGoodFiles(goodReadList);
                latch.countDown();
            }
        }.start();

        new Thread() {
            @Override
            public void run() {
                super.run();
                FileParser.parseOrderFiles(orderReadList);
                latch.countDown();
            }
        }.start();

        long end = System.currentTimeMillis();
        System.out.println((end - start) + " ms elapsed for starting reading task....");

        latch.await(); //wait for all thread complete

        end = System.currentTimeMillis();
        System.out.println((end - start) + "ms elapsed for Constructing. \n -----------  Parsing END -----------");

        for (String fileName : storeFolders) {
            System.out.println(fileName);
            file = new File(fileName);
            System.out.println("path: " + file.getAbsolutePath() + " size: " + file.length());

            //list files in current folder
            File[] files = file.listFiles();
            for (File f : files) {
                System.out.println("subpath: " + f.getAbsolutePath() + " size: " + f.length());
            }
        }
        System.out.println("");

        //reclaim some space in Java Heap
        runtime.gc();

        //TO DO: clean Direct Memory

        System.out.println("\n ---------- After Constructing: -----------");
        System.out.println("Max memory: " + runtime.maxMemory());
        System.out.println("Total memory: " + runtime.totalMemory());
        System.out.println("Free memory: " + runtime.freeMemory());

    }

    private Row createKVMapFromLine(String line) {
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = rawkv.substring(0, p);
            String value = rawkv.substring(p + 1);
            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            KV kv = new KV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }

    /**
     * @param orderId
     * @param keys    待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
     * @return
     */
    @Override
    public Result queryOrder(long orderId, Collection<String> keys) {
        System.out.println("OrderQuery " + (orderQueryCnt++) + " , id = " + orderId + ", Keys: ");
        if (keys != null) {
            for (String key : keys)
                System.out.print(orderId + " - " + key + ";");
            System.out.println("");
        } else {
            System.out.println("--Key is NULL ");
        }
        Result cacheResult;
        if ((cacheResult = TempCache.orderCache.get(orderId)) != null) {
            System.out.println("~~ hahahaha ~~");
            return cacheResult;
        }
        //fail fast, speed up testing

        long start = System.currentTimeMillis();
        int bucketId = 0;//(int) (orderId % FileParser.ORDER_BUCKET_NUM);   //hashed bucket
        //int bucketId =(int) ((orderId - FileParser.MIN) / FileParser.SIZE);

        OrderBucket bucket;
        boolean notFound = false;
        long fid_off;
        if ((bucket = FileParser.orderBucketList.get(bucketId)) != null) {

            Long fIdOffset = bucket.getOverFlowMap().get(orderId); //look first in overflow map
            if (fIdOffset != null) {
                fid_off = fIdOffset;
                System.out.println("hit in memory order id");
            }
            else {
                fid_off = bucket.loadIndex(orderId);   //load index from file
                if (fid_off == -1)
                    notFound = true;
            }

        } else {

            System.out.println("No such Order " + orderId);
            return null;

        }

        if (notFound) {
            System.out.println("After searching index in " + bucket.getStorePath() + ", No such Order " + orderId);
            return null;
        }

        long off = BitHelper.getOrderOffset(fid_off); //start from where
        byte fid = BitHelper.getOrderFileId(fid_off);

        String filename = FileParser.orderFIDMap.get(fid);    //get order store file name by id

        //read data from disk
        FastReader reader = new FastReader(new File(filename), off, FileParser.LINE_SIZE);
        byte[] data = reader.nextLine();
        if (data == null) {
            System.out.println("!! impossible, wrong index");
            return null;
        }

        String record = new String(data);
        Row orderRow = createKVMapFromLine(record);
        //System.out.println("-------We got order: " + orderRow);

        //if key set is empty,no field; null: get all joined fields, if keys are all in the key map, no need to join
        if (keys != null && keys.size() == 0) {
            long end = System.currentTimeMillis();
            ResultImpl result = new ResultImpl(orderId, new Row());
            System.out.println("==== Empty Key field Order Query: " + (end - start) + " , id = " + orderId +
                    " ms ==== MyResult " + result);
            return result; //empty fields;

        } else {

            Row buyerRow = null, goodRow = null;
            //check whether all keys are in order table
            boolean needJoin = false;
            Collection<String> currentKeys = createQueryKeys(orderRow.keySet());
            if (keys == null || !currentKeys.containsAll(keys))
                needJoin = true;
            if (needJoin) {
                KV buyer = orderRow.getKV("buyerid");
                String buyerId = buyer.rawValue;

                bucketId = Math.abs(buyerId.hashCode()) % FileParser.BUYER_BUCKET_NUM;

                GoodBuyerBucket goodBuyerBucket = null;
                Integer buyerFid_off = null;
                if ((goodBuyerBucket = FileParser.buyerBucketList.get(bucketId)) != null) {

                    buyerFid_off = goodBuyerBucket.relationMap.get(buyerId).offset;  //buyer & good's indices are in memory
                    System.out.println("buyerFid: "+buyerFid_off);
                }

                if (buyerFid_off == null) {
                    System.out.println("!! impossible, no such buyer");
                }else {

                    //System.out.println("before fid: "+fid+" , buyerFid_off: "+buyerFid_off);
                    fid = BitHelper.getGBFid(buyerFid_off);
                    int buyerOff = BitHelper.getGBoff(buyerFid_off);    //offset

                    filename = FileParser.buyerFIDMap.get(fid); //buyer's store file

                    //we need hash map index to get operator, then get offset
                /*
                Map<String, RelationOperator> buyerOrderMap = FileParser.buyerOrderRelationMaps.get(index);
                off = buyerOrderMap.get(buyerId).getOffset();*/

                    reader = new FastReader(new File(filename), buyerOff, FileParser.LINE_SIZE); //read a line
                    data = reader.nextLine();
                    reader.close();

                    if (data != null) {
                        record = new String(data);
                        buyerRow = createKVMapFromLine(record);
                        currentKeys.addAll(buyerRow.keySet());
                    }
                }

                //whether buyer row &　order row contains all keys, in order to reduce redundant query in disk
                if (keys == null || !currentKeys.containsAll(keys)) {

                    KV good = orderRow.getKV("goodid");
                    String goodId = good.rawValue;

                    bucketId = Math.abs(goodId.hashCode()) % FileParser.GOOD_BUCKET_NUM;
                    Integer goodFid_off = null;
                    if ((goodBuyerBucket = FileParser.goodBucketList.get(bucketId)) != null) {

                        goodFid_off = goodBuyerBucket.relationMap.get(goodId).offset;  //buyer & good's indices are in memory

                    }

                    if (goodFid_off == null) {
                        System.out.println("!! impossible, no such good");
                    }else {

                        fid = BitHelper.getGBFid(goodFid_off);
                        int goodOff = BitHelper.getGBoff(goodFid_off);    //offset

                        filename = FileParser.goodFIDMap.get(fid);

//                    Map<String, RelationOperator> goodOrderMap = FileParser.goodOrderRelationMaps.get(index);
//                    off = goodOrderMap.get(goodId).getOffset();

                        reader = new FastReader(new File(filename), goodOff, FileParser.LINE_SIZE); //read a line
                        data = reader.nextLine();
                        reader.close();
                        if (data != null) {
                            record = new String(data);
                            goodRow = createKVMapFromLine(record);
                        }
                    }
                }
            }

            long end = System.currentTimeMillis();
            ResultImpl result = ResultImpl.createResultRow(orderRow, buyerRow, goodRow, createQueryKeys(keys));
            System.out.println("====Order Query: " + (end - start) + " ms , id = " + orderId
                    + " ==== MyResult " + result);

            //a little trick % 8, block queryBuyer/Saler...
            /*if ((orderId & 7) == 0)
                FileParser.isBlock = false;*/
            return result;
        }

    }

    /**
     * 根据买家id 拿到该买家所有的订单，根据时间descending排序，all fields are needed
     *
     * @param startTime 订单创建时间的下界
     * @param endTime   订单创建时间的上界
     * @param buyerid   买家Id
     * @return
     */

    @Override
    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime, String buyerid) {
        System.out.println("TimePeriod Buyer's Orders " + (buyerQueryCnt++) + " , buyer id = "
                + buyerid + ", time: " + startTime + "-" + endTime);

        //while (FileParser.isBlock)
        //    ;//spin


        long start = System.currentTimeMillis();

        //get buyer's information, only one buyer
        int bucketId = Math.abs(buyerid.hashCode()) % FileParser.BUYER_BUCKET_NUM;
        GoodBuyerBucket bucket;
        RelationOperator buyerOrderRelation;
        int buyerFidOff;
        if((bucket = FileParser.buyerBucketList.get(bucketId)) != null
                && (buyerOrderRelation = bucket.relationMap.get(buyerid)) != null){

            buyerFidOff = buyerOrderRelation.offset;

        }else{

            System.out.println("--Not found buyerId: " + buyerid);
            List<Result> results = new ArrayList<Result>();
            return results.iterator();
        }

        //read buyer's info from disk
        byte buyerFid = BitHelper.getGBFid(buyerFidOff);
        int  buyerOff = BitHelper.getGBoff(buyerFidOff);

        String buyerFileName = FileParser.buyerFIDMap.get(buyerFid);

        FastReader reader = new FastReader(new File(buyerFileName), buyerOff, FileParser.LINE_SIZE); //read a line
        byte[] data = reader.nextLine();
        reader.close();

        String record = new String(data);
        Row buyerRow = createKVMapFromLine(record);
        Row goodRow = null, orderRow = null;

        List<Long> buyerOrdersList = bucket.loadOrderLocs(buyerid); //load index of order

        List<Result> resultList = new ArrayList<Result>(); // no need to sort, it has been done in tree map
        TreeMap<Long, Row> orderList = new TreeMap<Long, Row>(); //add row first, order by time, then join with other table

        for (long orderFidOff : buyerOrdersList) {

            byte orderFid = BitHelper.getOrderFileId(orderFidOff);
            long orderOff = BitHelper.getOrderOffset(orderFidOff);

            String orderFilename = FileParser.orderFIDMap.get(orderFid);

            reader = new FastReader(new File(orderFilename), orderOff, FileParser.LINE_SIZE); //read a line
            data = reader.nextLine();
            reader.close();

            if (data != null) {
                record = new String(data);
                orderRow = createKVMapFromLine(record);     //order row can't be null
            }

            //filter out 'createTime' not qualified
            long createTime = orderRow.getKV("createtime").longValue;
            if (createTime < startTime || createTime > endTime)
                continue;

            orderList.put(createTime, orderRow);  //sorted by create time (suggest: sorting when construct
        }

        //Join all row in list, descending order
        for (long createTime : orderList.descendingKeySet()) {

            orderRow = orderList.get(createTime);
            goodRow = null;

            KV good = orderRow.getKV("goodid");
            String goodId = good.rawValue;

            int goodBucketId = Math.abs(goodId.hashCode()) % FileParser.GOOD_BUCKET_NUM;

            int goodFidOff = FileParser.goodBucketList.get(goodBucketId).relationMap.get(goodId).offset;
            byte goodFid = BitHelper.getGBFid(goodFidOff);
            int goodOff  = BitHelper.getGBoff(goodFidOff);
            String goodFileName = FileParser.goodFIDMap.get(goodFid);

            reader = new FastReader(new File(goodFileName), goodOff, FileParser.LINE_SIZE); //read a line
            data = reader.nextLine();
            reader.close();

            //data can be null, unbelievalbe
            if (data != null) {
                record = new String(data);
                goodRow = createKVMapFromLine(record);
            }

            resultList.add(ResultImpl.createResultRow(orderRow, buyerRow, goodRow, createQueryKeys(null)));

        }

        long end = System.currentTimeMillis();
        System.out.println("======= TimePeriod query: " + (end - start) + " ms , buyer id = "
                + buyerid+ "; size: " + resultList.size() + " ===== MyResult List + resultList" );
        return resultList.iterator();

    }

    /**
     * 根据goodid，拿到该good的所有order所在位置，读取所有订单及其相关字段，返回整个ResultTreeMap, 以orderId排序
     *
     * @param salerid 卖家Id
     * @param goodid  商品Id  This is PK
     * @param keys    待查询的字段，如果为null，则查询所有字段，如果为空，则排除所有字段
     * @return
     */
    @Override
    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid, Collection<String> keys) {
        System.out.println("Good-Saler all orders " + (salerQueryCnt++) + " , good id: " + goodid + "  keys: ");
        if (keys != null) {
            for (String key : keys)
                System.out.println(goodid + " - " + key + ";");
            System.out.println("");
        } else {
            System.out.println("--Key is NULL ");
        }

        //while (FileParser.isBlock)
        //    ;//spin

        //get good's information by offset in file, only one good
        int bucketId = Math.abs(goodid.hashCode()) % FileParser.GOOD_BUCKET_NUM;
        GoodBuyerBucket goodBucket;
        RelationOperator operator;
        int goodFidOff;
        if((goodBucket = FileParser.goodBucketList.get(bucketId)) != null
                && (operator = goodBucket.relationMap.get(goodid)) != null){

            goodFidOff = operator.offset;

        }else{

            System.out.println("--Not found goodId: " + goodid);
            List<Result> results = new ArrayList<Result>();
            return results.iterator();

        }

        byte goodFid = BitHelper.getGBFid(goodFidOff);
        int  goodOff = BitHelper.getGBoff(goodFidOff);

        String goodFilename = FileParser.goodFIDMap.get(goodFid);

        FastReader reader = new FastReader(new File(goodFilename), goodOff, FileParser.LINE_SIZE); //read a line
        byte[] data = reader.nextLine();
        reader.close();

        if(data == null){
            System.out.println("!! impossible , no good in this line");
            List<Result> results = new ArrayList<Result>();
            return results.iterator();
        }

        String record = new String(data);
        Row goodRow = createKVMapFromLine(record);

        List<Long> goodOrderList = goodBucket.loadOrderLocs(goodid);
        Collection<String> currentKeys = createQueryKeys(goodRow.keySet());

        long start = System.currentTimeMillis();
        SortedSet<Result> resultList = new TreeSet<Result>(); //sorting result by order id
        boolean isEmpty = false;

        //Case key set:  empty =>no field;
        //              null => get all joined fields
        //     keys are all in the order key map => no need to join
        if (keys != null && keys.size() == 0) {
            isEmpty = true;
            System.out.println("empty key in Good Saler query");
        }
        if (isEmpty) {
            for (long fileOffset : goodOrderList) {
                byte orderFId = BitHelper.getOrderFileId(fileOffset);
                long orderOff = BitHelper.getOrderOffset(fileOffset);

                String orderFilename = FileParser.orderFIDMap.get(orderFId);

                reader = new FastReader(new File(orderFilename), orderOff, FileParser.LINE_SIZE); //read a line
                data = reader.nextLine();
                reader.close();

                record = new String(data);
                Row orderRow = createKVMapFromLine(record);
                long orderId = orderRow.getKV("orderid").longValue;

                ResultImpl result = new ResultImpl(orderId, new Row());
                resultList.add(result);
            }
            System.out.println("Empty keys for good-saler query , goodid "+goodid);

        } else {
            //check whether all keys are in order table
            boolean needJoin = true;
            for (long fileOffset : goodOrderList) {
                byte orderFId = BitHelper.getOrderFileId(fileOffset);
                long orderOff = BitHelper.getOrderOffset(fileOffset);

                String orderFilename = FileParser.orderFIDMap.get(orderFId);

                reader = new FastReader(new File(orderFilename), orderOff, FileParser.LINE_SIZE); //read a line
                data = reader.nextLine();
                reader.close();

                record = new String(data);
                Row orderRow = createKVMapFromLine(record);
                Row buyerRow = null;

                if (needJoin) {
                    currentKeys.addAll(orderRow.keySet());   //avoid unnecessary queries
                    if (currentKeys.containsAll(keys) && keys != null)
                        needJoin = false;
                }

                if (needJoin) {
                    KV buyer = orderRow.getKV("buyerid");
                    String buyerId = buyer.rawValue;

                    int buyerBucketId = Math.abs(buyerId.hashCode()) % FileParser.BUYER_BUCKET_NUM;

                    GoodBuyerBucket bucket;
                    RelationOperator buyerOrderRelation;
                    int buyerFidOff=-1;
                    if ((bucket = FileParser.buyerBucketList.get(buyerBucketId)) != null
                            && (buyerOrderRelation = bucket.relationMap.get(buyerId)) != null) {

                        buyerFidOff = buyerOrderRelation.offset;

                    }

                    if(buyerFidOff == -1){
                        System.out.println("!! WTF , no such buyer in bucket");
                    }
                    //read buyer's info from disk
                    byte buyerFid = BitHelper.getGBFid(buyerFidOff);
                    int buyerOff = BitHelper.getGBoff(buyerFidOff);

                    String buyerFileName = FileParser.buyerFIDMap.get(buyerFid);

                    reader = new FastReader(new File(buyerFileName), buyerOff, FileParser.LINE_SIZE); //read a line
                    data = reader.nextLine();
                    reader.close();

                    if(data != null)
                        buyerRow = createKVMapFromLine(new String(data));
                    else{
                        System.out.println("!! WTF, can't read anything in file for buyer "+buyerId);
                    }
                }

                ResultImpl result = ResultImpl.createResultRow(orderRow, buyerRow, goodRow, createQueryKeys(keys));
                resultList.add(result);
            }
        }

        long end = System.currentTimeMillis();
        System.out.println("==Good-Saler all orders: " + (end - start) + " ms," + " , good id = " + goodid +
                "; size: " + resultList.size()+ "; + resultList" );

        return resultList.iterator();
    }

    /**
     * find all orders by goodid, then return a result list, finally accumulate the required key
     *
     * @param goodid 商品Id
     * @param key    求和字段
     * @return
     */
    @Override
    public KeyValue sumOrdersByGood(String goodid, String key) {
        System.out.println("SumOrderQuery " + (sumQueryCnt++) + " , good id = " + goodid + " , key= " + key);

        if (goodid == null || key == null || goodid.length() == 0 || key.length() == 0)
            return null;

        KV cacheKV;
        if ((cacheKV = TempCache.sumCache.get(goodid)) != null) {
            System.out.println(" ~~ haha ~~");
            return cacheKV;
        }
        long start = System.currentTimeMillis();

        //while (FileParser.isBlock)
          //  ;//spin

        //get good's information by offset in file, only one good
        int bucketId = Math.abs(goodid.hashCode()) % FileParser.GOOD_BUCKET_NUM;
        GoodBuyerBucket goodBucket;
        RelationOperator operator;
        int goodFidOff;
        if((goodBucket = FileParser.goodBucketList.get(bucketId)) != null
                && (operator = goodBucket.relationMap.get(goodid)) != null){

            goodFidOff = operator.offset;

        }else{

            System.out.println("--Not found goodId: " + goodid);
            return null;
        }

        byte goodFid = BitHelper.getGBFid(goodFidOff);
        int  goodOff = BitHelper.getGBoff(goodFidOff);

        String goodFilename = FileParser.goodFIDMap.get(goodFid);

        FastReader reader = new FastReader(new File(goodFilename), goodOff, FileParser.LINE_SIZE); //read a line
        byte[] data = reader.nextLine();
        reader.close();

        if(data == null){
            System.out.println("!! impossible , no good in this line");
            return null;
        }

        String record = new String(data);
        Row goodRow = createKVMapFromLine(record);

        List<Long> goodOrderList = goodBucket.loadOrderLocs(goodid);
        Collection<String> currentKeys = createQueryKeys(goodRow.keySet());

        //if this key only exist in this good, then just return it
        if (currentKeys.contains(key)) {
            KV kv = goodRow.getKV(key);
            try {
                boolean hasValidData = false;
                long sum = 0;
                if (kv != null) {
                    sum += kv.valueAsLong();
                    hasValidData = true;
                }
                if (hasValidData) {
                    long end = System.currentTimeMillis();
                    System.out.println("== Sum Query : " + (end - start) + " ms, Long: " + sum + " , good id = " + goodid);
                    return new KV(key, Long.toString(sum));
                }
            } catch (TypeException e) {
                System.out.println(e + "  Not long value, go to Double accumulate");
            }

            // accumulate as double
            try {
                boolean hasValidData = false;
                double sum = 0;
                if (kv != null) {
                    sum += kv.valueAsDouble();
                    hasValidData = true;
                }
                if (hasValidData) {
                    long end = System.currentTimeMillis();
                    System.out.println("== Sum Query : " + (end - start) + " ms, Double: " + sum + " , good id = " + goodid);
                    return new KV(key, Double.toString(sum));
                }
            } catch (TypeException e) {
            }
        }

        boolean needJoin = true; //whenever this field occurs, reduce unnecessary IO operations, check FILE TYPE
        List<Result> resultList = new ArrayList<Result>(80);
        //get all orders related to this good
        for (long fileOffset : goodOrderList) {
            byte orderFId = BitHelper.getOrderFileId(fileOffset);
            long orderOff = BitHelper.getOrderOffset(fileOffset);

            String orderFilename = FileParser.orderFIDMap.get(orderFId);

            reader = new FastReader(new File(orderFilename), orderOff, FileParser.LINE_SIZE); //read a line
            data = reader.nextLine();
            reader.close();

            record = new String(data);
            Row orderRow = createKVMapFromLine(record);
            Row buyerRow = null;

            if (needJoin) {
                currentKeys.addAll(orderRow.keySet());   //avoid unnecessary queries
                if (currentKeys.contains(key) )
                    needJoin = false;
            }

            if (needJoin) {
                KV buyer = orderRow.getKV("buyerid");
                String buyerId = buyer.rawValue;

                int buyerBucketId = Math.abs(buyerId.hashCode()) % FileParser.BUYER_BUCKET_NUM;

                GoodBuyerBucket bucket;
                RelationOperator buyerOrderRelation;
                int buyerFidOff=-1;
                if ((bucket = FileParser.buyerBucketList.get(buyerBucketId)) != null
                        && (buyerOrderRelation = bucket.relationMap.get(buyerId)) != null) {

                    buyerFidOff = buyerOrderRelation.offset;

                }

                if(buyerFidOff == -1){
                    System.out.println("!! WTF , no such buyer in bucket");
                }
                //read buyer's info from disk
                byte buyerFid = BitHelper.getGBFid(buyerFidOff);
                int buyerOff = BitHelper.getGBoff(buyerFidOff);

                String buyerFileName = FileParser.buyerFIDMap.get(buyerFid);

                reader = new FastReader(new File(buyerFileName), buyerOff, FileParser.LINE_SIZE); //read a line
                data = reader.nextLine();
                reader.close();

                if(data != null)
                    buyerRow = createKVMapFromLine(new String(data));
                else{
                    System.out.println("!! WTF, can't read anything in file for buyer "+buyerId);
                }
            }

            List<String> keyList = new ArrayList<String>(1);
            keyList.add(key);
            ResultImpl result = ResultImpl.createResultRow(orderRow, buyerRow, goodRow, createQueryKeys(keyList));
            resultList.add(result);
        }

        //construct sum function according to queried result
        // accumulate as Long
        try {
            boolean hasValidData = false;
            long sum = 0;
            for (Result r : resultList) {
                KeyValue kv = r.get(key);
                if (kv != null) {
                    sum += kv.valueAsLong();
                    hasValidData = true;
                }
            }
            if (hasValidData) {
                long end = System.currentTimeMillis();
                System.out.println("== Sum Query : " + (end - start) + " ms, Long: " + sum + " , good id = " + goodid);
                return new KV(key, Long.toString(sum));
            }
        } catch (TypeException e) {
            System.out.println(e + "  \n --- Not long value, go to Double accumulate");
        }

        // accumulate as double
        try {
            boolean hasValidData = false;
            double sum = 0;
            for (Result r : resultList) {
                KeyValue kv = r.get(key);
                if (kv != null) {
                    sum += kv.valueAsDouble();
                    hasValidData = true;
                }
            }
            if (hasValidData) {
                long end = System.currentTimeMillis();
                System.out.println("== Sum Query : " + (end - start) + " ms, Double: " + sum + " , good id = " + goodid);
                return new KV(key, Double.toString(sum));
            }
        } catch (TypeException e) {
        }

        return null;
    }


    public static void main(String args[]) throws IOException, InterruptedException {
        // init order system
        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        ;
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();

        orderFiles.add("order_records.txt");
        buyerFiles.add("buyer_records.txt");
        goodFiles.add("good_records.txt");
        storeFolders.add("E://test/");

        OrderSystem os = new OrderSystemImpl_Demo();
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 用例
        long orderid = 2982388;
        System.out.println("\n查询订单号为" + orderid + "的订单");
        System.out.println(os.queryOrder(orderid, null));

        System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
        System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

        System.out.println("\n查询订单号为" + orderid
                + "的订单的contactphone, buyerid, foo, done, price字段");
        List<String> queryingKeys = new ArrayList<String>();
        queryingKeys.add("contactphone");
        queryingKeys.add("buyerid");
        queryingKeys.add("foo");
        queryingKeys.add("done");
        queryingKeys.add("price");
        Result result = os.queryOrder(orderid, queryingKeys);
        System.out.println(result);
        System.out.println("\n查询订单号不存在的订单");
        result = os.queryOrder(1111, queryingKeys);
        if (result == null) {
            System.out.println(1111 + " order not exist");
        }

        String buyerid = "tb_a99a7956-974d-459f-bb09-b7df63ed3b80";
        long startTime = 1471025622;
        long endTime = 1471219509;
        System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
        Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        String goodid = "good_842195f8-ab1a-4b09-a65f-d07bdfd8f8ff";
        String salerid = "almm_47766ea0-b8c0-4616-b3c8-35bc4433af13";
        System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
        it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        goodid = "good_d191eeeb-fed1-4334-9c77-3ee6d6d66aff";
        String attr = "app_order_33_0";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        System.out.println(os.sumOrdersByGood(goodid, attr));

        attr = "done";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        KeyValue sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段是布尔类型，返回值是null");
        }

        attr = "foo";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段不存在，返回值是null");
        }
    }
}
