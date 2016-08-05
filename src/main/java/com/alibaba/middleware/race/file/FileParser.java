package com.alibaba.middleware.race.file;

import com.alibaba.middleware.race.OrderSystem;
import com.alibaba.middleware.race.OrderSystemImpl;
import com.alibaba.middleware.race.model.*;
import com.alibaba.middleware.race.util.FastAppender;
import com.alibaba.middleware.race.util.FastReader;

import java.io.*;
import java.util.*;
import java.util.concurrent.*;


/**
 * Created by zhaoz on 2016/7/16.
 * 读取文件
 * 根据PK hash到指定的bucket
 * bucket快满flush到disk
 * 记录record及其offset
 */
public class FileParser {
    public static final byte ORDER = 0;
    public static final byte BUYER = 0X01;
    public static final byte GOOD = 0X02;

    public static final int LINE_SIZE = 4096;        //4k one page for a LINE, enough for most cases
    public static final int BUF_SIZE = 100 << 20;     //mappedByteBuffer size, direct memory, 40MB for about 20 reader
    public static final int GOOD_ORDER_NUM = 50;    //a good has 50 order on average
    public static final int BUYER_ORDER_NUM = 100;

    public static final long ORDER_THRESHOLD = 2L << 30;    //单个订单文件如果超过该值需要split
    public static final long BUYER_THRESHOLD = 500L << 20;
    public static final long GOOD_THRESHOLD = 500L << 20;

    public static final int O_BUFF_SIZE = 15 << 20;     //in-memory bucket write buffer size,
    public static final int ORDER_BUCKET_NUM = 30;     //how many bucket can be hashed to
    public static final int O_RECORD_NUM = 600000;     //2mill, 32MB, 3times flush

    public static final int LAST_SLICE = 400000 / (O_RECORD_NUM * ORDER_BUCKET_NUM) - 1;

    public static final int BUYER_BUCKET_SIZE = 1 << 20; //BUYER
    public static final int BUYER_BUCKET_NUM = 2;
    public static final int B_RECORD_NUM = 3000;

    public static final int GOOD_BUCKET_SIZE = 2 << 20; //GOOD
    public static final int GOOD_BUCKET_NUM = 2;
    public static final int G_RECORD_NUM = 5000;

    public static final int B_O_CACHE_SIZE = 1024;
    public static final int G_O_CACHE_SIZE = 1024;

    //order, buyer, good data bucket list
    public static final List<OrderBucket> orderBucketList = new ArrayList<OrderBucket>(ORDER_BUCKET_NUM);
    public static final List<GoodBuyerBucket> buyerBucketList = new ArrayList<GoodBuyerBucket>(BUYER_BUCKET_NUM);
    public static final List<GoodBuyerBucket> goodBucketList = new ArrayList<GoodBuyerBucket>(GOOD_BUCKET_NUM);

    //file <id, name> map
    public static final HashMap<Byte, String> orderFIDMap = new HashMap<Byte, String>(64, 0.9f);
    public static final HashMap<Byte, String> buyerFIDMap = new HashMap<Byte, String>();
    public static final HashMap<Byte, String> goodFIDMap = new HashMap<Byte, String>();

    //block flag
    public static volatile boolean isBlock = true;

    // overflow cache
    /*public static ConcurrentHashMap<String, RelationOperator> buyerOrderCacheMap =
            new ConcurrentHashMap<String, RelationOperator>(B_O_CACHE_SIZE);
    public static ConcurrentHashMap<String, RelationOperator> goodOrderCacheMap =
            new ConcurrentHashMap<String, RelationOperator>(G_O_CACHE_SIZE);*/

    /**
     * 获得一条记录中的主键
     *
     * @param record
     * @param type
     * @return
     */
    public static String retrievePK(String record, byte type) {
        if (record == null)
            return null;
        String pk = null;
        String[] pairs = record.split("\t");

        switch (type) {
            case ORDER:
                for (String pair : pairs) {
                    if (pair.contains("orderid")) {
                        pk = pair.substring(pair.indexOf(":") + 1);
                        break;
                    }
                }
                break;
            case BUYER:
                for (String pair : pairs) {
                    if (pair.contains("buyerid")) {
                        pk = pair.substring(pair.indexOf(":") + 1);
                        break;
                    }
                }
                break;
            case GOOD:
                for (String pair : pairs) {
                    if (pair.contains("goodid")) {
                        pk = pair.substring(pair.indexOf(":") + 1);
                        break;
                    }
                }
                break;
        }
        return pk;
    }

    //take out the meaningful bit
    public static int getFileId(long file_offset) {
        return (int) (file_offset >> 32); //32~44th bit
    }

    public static int getOffset(long file_offset) {
        return (int) file_offset; //0~31th bit
    }

    public static void init(String orderStore, String goodStore, String buyerStore) {
        for (int i = 0; i < ORDER_BUCKET_NUM; i++) {
            OrderBucket bucket = new OrderBucket(i, orderStore + "order_" + i, O_BUFF_SIZE, O_RECORD_NUM);
            orderBucketList.add(bucket);
        }

        for (int i = 0; i < BUYER_BUCKET_NUM; i++) {
            GoodBuyerBucket bucket = new GoodBuyerBucket(BUYER, buyerStore + "buyer_" + i, BUYER_BUCKET_SIZE, B_RECORD_NUM, BUYER_ORDER_NUM);
            buyerBucketList.add(bucket);
        }

        for (int i = 0; i < GOOD_BUCKET_NUM; i++) {
            GoodBuyerBucket bucket = new GoodBuyerBucket(GOOD, goodStore + "good_" + i, GOOD_BUCKET_SIZE, G_RECORD_NUM, GOOD_ORDER_NUM);
            goodBucketList.add(bucket);
        }
    }
    //sample
    public static volatile long maxOrderId=0;
    public static volatile long minOrderId=Long.MAX_VALUE;
    public static final long MAX =627919339; //60767378408L;  //true data
    public static final long MIN = 587732167;//587732231;
    public static final long SIZE = (int)((MAX - MIN) / ORDER_BUCKET_NUM) + 1; // 60 BUCKET < 2147483648

    //calculate 9, 10, 11 digit orderid number
    public static volatile long Y1 = 0;
    public static volatile long Y2 = 0;
    public static volatile long Y3 = 0;
    public static volatile long Y4 = 0;
    public static volatile long Y5 = 0;
    public static volatile long Y6 = 0;
    public static volatile long Y7 = 0;
    public static volatile long Y8 = 0;
    public static volatile long Y9 = 0;
    public static volatile long D8 = 0;
    public static volatile long Y100 = 0;
    public static volatile long Y200 = 0;
    public static volatile long Y300 = 0;
    public static volatile long Y400 = 0;
    public static volatile long Y500 = 0;
    public static volatile long Y600 = 0;



    //--------------------------------------Fast File Reading----------------------------------------------
    //输入文件, 一块块地映射进内存buffer，记录<PK, fileId_offset>，放到指定bucket, 异步写索引文件

    public static void parseOrderFiles(Collection<String> inFiles) {

        //init
        final long start = System.currentTimeMillis();

        ExecutorService parseExecutor = Executors.newFixedThreadPool(4); //disk by disk parsing

        for (final String filename : inFiles) {
            //for each file, open a thread to handle I/O
          //  parseExecutor.execute(new Runnable() {
         //       @Override
          //      public void run() {

                    File inFile = new File(filename);

                    long begin =  System.currentTimeMillis();;
                    byte fileId=0;
                    for(Map.Entry<Byte, String> nameId : FileParser.orderFIDMap.entrySet()) {
                        if (nameId.getValue().equals(filename)) {
                            fileId = nameId.getKey();
                        }
                    }

                    FastReader fileReader = new FastReader(inFile, 0, BUF_SIZE);
                    byte[] line;
                    long offset = 0L;   //0-32 bit as offset, 33-39 as fileID

                    while ((line = fileReader.nextLine()) != null) {
                        String str = new String(line);
                        String pk = retrievePK(str, ORDER);
                        long orderId = Long.parseLong(pk.trim());   //trim string
                        int bucketId = 0;//(int) (orderId % ORDER_BUCKET_NUM);
                       // int bucketId =(int) ((orderId - MIN) / SIZE); //put in a range

//                        if(orderId < minOrderId){
//                            minOrderId = orderId;
//                        }else if (orderId > maxOrderId){
//                            maxOrderId = orderId;
//                        }

                        /*//remember to del
                        if(orderId > 60000000000l)
                            Y600 ++;
                        else if(orderId > 50000000000l)
                            Y500++;
                        else if(orderId > 40000000000l)
                            Y400++;
                        else if(orderId > 30000000000l)
                            Y300++;
                        else if(orderId > 20000000000l)
                            Y200++;
                        else if(orderId > 10000000000l)
                            Y100++;
                        else if (orderId > 9000000000l)
                            Y9 ++;
                        else if (orderId > 8000000000l)
                            Y8++;
                        else if (orderId > 7000000000l)
                            Y7++;
                        else if (orderId > 6000000000l)
                            Y6++;
                        else if (orderId > 5000000000l)
                            Y5++;
                        else if (orderId > 4000000000l)
                            Y4++;
                        else if (orderId > 3000000000l)
                            Y3++;
                        else if (orderId > 2000000000l)
                            Y2++;
                        else if (orderId > 1000000000l)
                            Y1++;
                        else
                            D8 ++;
*/

                        orderBucketList.get(bucketId).put(orderId, fileId, offset); //put record to the right bucket

                        //add order's location info to relevant good or buyer
//                        String goodId  = retrievePK(str, GOOD);
//                        bucketId = Math.abs(goodId.hashCode()) % GOOD_BUCKET_NUM;
//                        //goodBucketList.get(bucketId).putOrderLoc(goodId, offset, fileId);
//
//                        String buyerId = retrievePK(str, BUYER);
//                        bucketId = Math.abs(buyerId.hashCode()) % BUYER_BUCKET_NUM;
                        //buyerBucketList.get(bucketId).putOrderLoc(buyerId, offset, fileId);

                        offset += line.length; //record offset

                    }
                    fileReader.close();

                    System.out.println((System.currentTimeMillis() - begin) + "ms--- COMPLETE PARSING-- " + filename);
                }

           // });
        //}

        //when all file has finished hashing to bucket, start to writing index file
//        try {

        //    parseExecutor.shutdown();
        //    parseExecutor.awaitTermination(52, TimeUnit.MINUTES); //terminate in case time out

            for (OrderBucket bucket : orderBucketList)
                bucket.cleanUp();   //TO DO: load part of data into memory after constructing

            System.out.println("==========MAX ORDER ID: "+ maxOrderId);
            System.out.println("==========MIN ORDER ID: "+ minOrderId);
            System.out.println("==========Y9 ORDER NUM: "+ Y9);
            System.out.println("==========Y8 ORDER NUM: "+ Y8);
            System.out.println("==========Y7 ORDER NUM: "+ Y7);
            System.out.println("==========Y6 ORDER NUM: "+ Y6);
            System.out.println("==========Y5 ORDER NUM: "+ Y5);
            System.out.println("==========Y4 ORDER NUM: "+ Y4);
            System.out.println("==========Y3 ORDER NUM: "+ Y3);
            System.out.println("==========Y2 ORDER NUM: "+ Y2);
            System.out.println("==========Y1 ORDER NUM: "+ Y1);
            System.out.println("==========D8 ORDER NUM: "+ D8);
            System.out.println("==========Y100 ORDER NUM: "+ Y100);
            System.out.println("==========Y200 ORDER NUM: "+ Y200);
            System.out.println("==========Y300 ORDER NUM: "+ Y300);
            System.out.println("==========Y400 ORDER NUM: "+ Y400);
            System.out.println("==========Y500 ORDER NUM: "+ Y500);
            System.out.println("==========Y600 ORDER NUM: "+ Y600);

            //writing associated map
            //String savePath = storePath.substring(0, storePath.lastIndexOf('/'));
//            String buyer_order_prefix = orderStorePath+"buyer_order_";
//            String good_order_prefix = orderStorePath+"good_order_";
//            ObjectOutputStream objectOutputStream;
//            for (int i = 0; i < Bucket.buyerOrderMaps.size(); i++) {
//                String filename = buyer_order_prefix+i;
//                objectOutputStream = new ObjectOutputStream(new FileOutputStream(filename));
//                objectOutputStream.writeObject(Bucket.buyerOrderMaps.get(i));
//                objectOutputStream.close();
//            }
//
//            for (int i = 0; i < Bucket.goodOrderMaps.size(); i++) {
//                String filename = good_order_prefix+i;
//                objectOutputStream = new ObjectOutputStream(new FileOutputStream(filename));
//                objectOutputStream.writeObject(Bucket.goodOrderMaps.get(i));
//                objectOutputStream.close();
//            }

//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

        long end = System.currentTimeMillis();
        System.out.println((end - start) + "MS elapsed for ORDER files parsing "
                + inFiles);
    }


    /**
     * Parsing buyer files & saving them in small file & build indices
     *
     * @param inFiles
     */
    public static void parseBuyerFiles(Collection<String> inFiles) {
        long start = System.currentTimeMillis();

        System.out.println("BUYER FID MAP: "+buyerFIDMap);
        ExecutorService parseExecutor = Executors.newFixedThreadPool(2); //disk by disk parsing
        for (final String filename : inFiles) {
            //for each file, open a thread to handle I/O
            parseExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    File inFile = new File(filename);
                    long begin =  System.currentTimeMillis();;
                    FastReader fileReader = new FastReader(inFile, 0, BUF_SIZE);
                    byte[] line;
                    byte fileId=0;
                    for(Map.Entry<Byte, String> nameId : FileParser.buyerFIDMap.entrySet()) {
                        if (nameId.getValue().equals(filename)) {
                            fileId = nameId.getKey();
                        }
                    }
                    int curPos = 0; // < 2G

                    System.out.println("--Buyer fileId: "+fileId+" , name: "+filename);
                    while ((line = fileReader.nextLine()) != null) {
                        String str = new String(line).trim();
                        String buyerId = retrievePK(str, BUYER);

                        int bucketId = Math.abs(buyerId.hashCode()) % BUYER_BUCKET_NUM;
                        //Record record = new Record(buyerId, line);

                        buyerBucketList.get(bucketId).putLoc(buyerId, fileId, curPos); //put record to the right bucket
                        curPos += line.length;

                    }
                    fileReader.close();

                    System.out.println((System.currentTimeMillis() - begin) + "ms--- COMPLETE PARSING-- " + filename);

                }

            });
        }

        //when all file has finished hashing to bucket, start to writing index file
        try {
            parseExecutor.shutdown();
            parseExecutor.awaitTermination(30, TimeUnit.MINUTES);

    //        for (GoodBuyerBucket bucket : buyerBucketList)
//                bucket.cleanUp();
       //     System.out.println("buyer bucket "+bucket.storePath+" , size: "+bucket.relationMap.size());

        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms elapsed for BUYER files parsing "
                + inFiles);
    }

    public static void parseGoodFiles(Collection<String> inFiles) {
        long start = System.currentTimeMillis();
        System.out.println("GOOD FID MAP: "+goodFIDMap);
        ExecutorService parseExecutor = Executors.newFixedThreadPool(2); //disk by disk parsing
        for (final String filename : inFiles) {
            //for each file, open a thread to handle I/O
            parseExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    File inFile = new File(filename);
                    long begin =  System.currentTimeMillis();
                    FastReader fileReader = new FastReader(inFile, 0, BUF_SIZE);
                    byte[] line;
                    byte fileId=0;
                    for(Map.Entry<Byte, String> nameId : FileParser.goodFIDMap.entrySet()) {
                        if (nameId.getValue().equals(filename)) {
                            fileId = nameId.getKey();
                        }
                    }
                    int curPos = 0;     //less than 2GB
                    System.out.println("--Good fileId: "+fileId+" , name: "+filename);
                    while ((line = fileReader.nextLine()) != null) {
                        String str = new String(line).trim();
                        String goodId = retrievePK(str, GOOD);

                        int bucketId = Math.abs(goodId.hashCode()) % GOOD_BUCKET_NUM;
                        //Record record = new Record(goodId, line);

                        goodBucketList.get(bucketId).putLoc(goodId, fileId, curPos); //put record to the right bucket
                        curPos += line.length;

                    }
                    fileReader.close();

                    System.out.println((System.currentTimeMillis() - begin) + "ms--- COMPLETE PARSING-- " + filename);

                }

            });
        }

        //when all file has finished hashing to bucket, start to writing index file
        try {

            parseExecutor.shutdown();
            parseExecutor.awaitTermination(30, TimeUnit.MINUTES);

//            for (GoodBuyerBucket bucket : goodBucketList)
//                bucket.cleanUp();
//                System.out.println("good bucket "+bucket.storePath+" , size: "+bucket.relationMap.size());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        long end = System.currentTimeMillis();
        System.out.println((end - start) + "ms elapsed for good files parsing "
                + inFiles);
    }




    //no need to split file, because we read file in a buffer, not mapping the entire file
    //every time the buffer is full, we move the slide window of buffer to the next zone
    //keep reading the next line until the file end

    /**
     * 返回合适的文件拆分个数
     *
     * @param size
     * @return
     */
    private static int getSplitNum(long size, long threshold) {
        int num = (int) (size / threshold);
        if (0 == num)
            num = 1;
        int remainder = (int) (size % threshold);
        //超过 1/4 则再建立一个文件， 否则追加到最后一个文件当中
        if (remainder > (threshold >> 2))
            num++;
        return num;
    }


    /**
     * 拆分文件
     * 按标准文件大小拆分
     * 拆分好之后排序输出
     * 注意结尾处的处理：
     * 记住上一条记录的结束位置，
     * 如果到最后未读到尾部，记录是断裂的，回到这条记录的起始位置
     *
     * @param inputPath
     */
    private static void splitOrderFileAndHash(String inputPath) {
        File file = new File(inputPath);
        int splitNum = getSplitNum(file.length(), ORDER_THRESHOLD);  //得到切分文件数
        long startPos = 0;

        //first several file is standard
        long count = 0;
        System.out.println("Spliting file: " + inputPath + " into " + splitNum);
        FastReader fileReader = null;

        for (int i = 0; i < splitNum; i++) {
            fileReader = new FastReader(file, startPos, BUF_SIZE); //map file into memory
            byte[] line = null;

            while (true) {
                startPos = fileReader.getCurrPos(); //the start position of this record
                line = fileReader.nextLine();

                if (line == null || startPos > ORDER_THRESHOLD) {
                    //reach the end of the MappedBuffer, while it's not a complete line, step backward to the last line
                    break;
                }

                String str = new String(line);
                String pk = retrievePK(str, ORDER);
                long orderId = Long.parseLong(pk);
                int bucketId = (int) (orderId % ORDER_BUCKET_NUM);
                Record record = new Record(orderId, line);

                //orderBucketList.get(bucketId).put(record); //put record to the right bucket
            }
/*
            count += startPos;
            if (i == splitNum - 1)
                size = (int) (file.length() - count);*/ //remaining file length in the last slice
        }

    }

    public static void splitBuyerFileAndHash(String filename) {
        File file = new File(filename);
        int splitNum = getSplitNum(file.length(), BUYER_THRESHOLD);  //得到切分文件数
        long startPos = 0L;
        int size = BUYER_BUCKET_SIZE;   //first several file is standard
        long count = 0;
        System.out.println("Spliting file: " + filename + " into " + splitNum);
        FastReader fileReader = null;

        for (int i = 0; i < splitNum; i++) {
            fileReader = new FastReader(file, startPos, BUF_SIZE); //map file into memory
            byte[] line = null;

            while (true) {
                startPos = fileReader.getCurrPos(); //the start position of this record
                line = fileReader.nextLine();

                if (line == null) {
                    //reach the end of the MappedBuffer, while it's not a complete line, step backward to the last line
                    break;
                }

                String str = new String(line);
                String pk = retrievePK(str, BUYER);

                int bucketId = Math.abs(pk.hashCode()) % BUYER_BUCKET_NUM;
                Record record = new Record(pk, line);

                //buyerBucketList.get(bucketId).put(record); //put record to the right bucket
            }

            count += startPos;
            if (i == splitNum - 1)
                size = (int) (file.length() - count); //remaining file length in the last slice
        }
    }


    public static void splitGoodFileAndHash(String filename) {
        File file = new File(filename);
        int splitNum = getSplitNum(file.length(), GOOD_THRESHOLD);  //得到切分文件数
        long startPos = 0;
        int size = GOOD_BUCKET_SIZE;   //first several file is standard
        long count = 0;
        System.out.println("Spliting file: " + filename + " into " + splitNum);
        FastReader fileReader = null;

        for (int i = 0; i < splitNum; i++) {
            fileReader = new FastReader(file, startPos, BUF_SIZE); //map file into memory
            byte[] line = null;

            while (true) {
                startPos = fileReader.getCurrPos(); //the start position of this record
                line = fileReader.nextLine();

                if (line == null || startPos > GOOD_THRESHOLD) {
                    //reach the end of the MappedBuffer, while it's not a complete line, step backward to the last line
                    break;
                }

                String str = new String(line);
                String pk = retrievePK(str, GOOD);

                int bucketId = Math.abs(pk.hashCode()) % GOOD_BUCKET_NUM;
                Record record = new Record(pk, line);

                //goodBucketList.get(bucketId).put(record); //put record to the right bucket
            }

            count += startPos;
            if (i == splitNum - 1)
                size = (int) (file.length() - count); //remaining file length in the last slice
        }
    }


    public static void main(String[] args) {
       Collection<String> inFiles = new ArrayList<String>();
        Collection<String> outFiles = new ArrayList<String>();
        inFiles.add("order_records.txt");
        outFiles.add("./");
        long start = System.currentTimeMillis();
        orderFIDMap.put((byte)0, "order_records.txt");
        OrderBucket bucket = new OrderBucket(0, "order_test" , 2560, 40);
        orderBucketList.add(bucket);
        parseOrderFiles(inFiles);
        OrderSystemImpl os = new OrderSystemImpl();
        for (int i = 3008479; i < 3008509; i++) {
            os.queryOrder(i , new ArrayList<String>());
        }

/*        List<String> files = new ArrayList<String>();
        files.add("2982138_2995952");
        files.add("2995954_3009311");

        mergeFile(files, "./");*//*
        long end = System.currentTimeMillis();
        System.out.println("Merging two file use traditional way takes: " + (end - start));*/

        //retrieve key from record, save in a list, then search it, check manually, finally write in several chunks
//        FastReader fileReader = new FastReader(new File("order_records.txt"));
//        int pos = 1240;
//        FastAppender appender = new FastAppender(new File("append.txt"), pos, 2 << 20);
//        byte[] line = null;
//        Map<Comparable, Integer> offset = new HashMap<Comparable, Integer>(2048);
//
//        int cnt = 0;
//        String[] orders = {
//                "orderid:29821381	goodid:aliyun_b5df3188-7439-4d79-9be4-190fa3796111",
//                "orderid:29821391	goodid:goodxn_9343708b-4ff2-473b-a83c-8f19d74028f6",
//                "orderid:29821401	goodid:aliyun_ada1969a-e3bd-4788-a20c-312c41265e6b",
//                "orderid:29821411	goodid:goodtb_2b0d960d-1917-42ec-b344-1b485988b8e5",
//                "orderid:29821431	goodid:goodtb_b8facf0a-abb9-4f78-b33a-e104e640dc24",
//                "orderid:29822101	goodid:goodxn_e6d175fc-6b64-42ad-b31d-368f6ef86bf3",
//                "orderid:29822111	goodid:goodal_ac94d362-63f8-4ae8-8170-be20b73a3d0a",
//                "orderid:29822121	goodid:goodxn_092aa44e-5830-43d2-95fb-aa1640cc555b",
//                "orderid:29822141	goodid:good_b919a5c5-edc6-48c1-b5f6-d02fb58451aa",
//                "orderid:29822151	goodid:good_dce13536-552f-44c7-9187-92738ba583d6",
//                "orderid:29822621"};
//        long start = System.currentTimeMillis();
//        while ((line = fileReader.nextLine()) != null) {
//            // while(cnt ++ < orders.length-1){
//            String str = new String(line);//orders[cnt];
//            String pk = retrievePK(str, ORDER);
//            long orderId = Long.parseLong(pk);
//            offset.put(orderId, pos);
//            // str +='\n';
//            // line = str.getBytes();
//            appender.append(line);
//            //  System.out.println(orderId+" : "+pos);
//            pos = (int) appender.getCurrPos();   //save every position
//           /*// int bucketId = (int) (orderId % BUCKET_NUM);
//            Record record = new Record(orderId, line);
//
//            orderBucketList.get(bucketId).put(record); //put record to the right bucket*/
//        }
//        pos = (int) appender.getCurrPos();
//        long end = System.currentTimeMillis();
//        System.out.println("Write Bytes: " + pos + " Spend " + (end - start));
//        fileReader.close();
//        appender.close();
//        long order = 2982138;
//        long off = offset.get(order);
//        System.out.println(order + " : " + off);
//
//        long order1 = 3003130;
//        long off1 = offset.get(order1);
//        System.out.println(order1 + " : " + off1);
//        start = end;
//        fileReader = new FastReader(new File("append.txt"), off1, 1024);
//        line = fileReader.nextLine();
//        end = System.currentTimeMillis();
//        System.out.println(order1 + " :  " + new String(line) + "\n Searching takes " + (end - start));
//
//        start = end;
//        line = fileReader.exactLine(off);   //continuous reading
//        end = System.currentTimeMillis();
//        System.out.println(order + " :  " + new String(line) + "\n Searching takes " + (end - start));
//        long s = 2945720001L;
//        System.out.println(ORDER_THRESHOLD);
//        System.out.println("num: " + getSplitNum(s, ORDER_THRESHOLD));
//        long l = 3829657015l;
//        System.out.println("num: " + getSplitNum(l, ORDER_THRESHOLD));
    }
}
