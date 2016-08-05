package com.alibaba.middleware.race.util;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

/**
 * Created by zhaoz on 2016/7/18.
 */
public class FastWriter {
    private FileChannel channel;
    private MappedByteBuffer buffer;

    /**
     * writing known size large data to a file, using memory mapping is a shot
     *
     * @param file
     * @param size
     */
    public FastWriter(File file, long size) {
        try {
            this.channel = FileChannel.open(file.toPath(),
                    StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.READ);
            this.buffer = channel.map(FileChannel.MapMode.READ_WRITE, 0, size);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void write(byte[] data) {
        buffer.put(data);
    }

    public void close() {
        try {
            channel.force(false);   //in case data loss
            buffer = null;
            channel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) throws IOException {

        //test file appending
        int size = 100;//100 << 20; //100MB
        int lineNum = 5;
        int cnt = 0;
        byte[] data = new byte[size / lineNum];
        for (int i = 0; i < data.length; i++)
            data[i] = (byte) ('m');

        /*String storePath2 = "../2995954_3009311";
        long start = System.currentTimeMillis();
        File file = new File(storePath2);
        FastWriter writer = new FastWriter(file, size+lineNum);//we know exactly how many data will be written

        while (cnt++ < lineNum) {
            writer.write(data);
            byte[] a = new byte[1];
            a[0] = '\n';
            writer.write(a);
        }
        writer.close();
        long end = System.currentTimeMillis();
        System.out.println("\n Mapped ByteBuffer Writing time: " + (end - start) + "ms, size: " + file.length() + " Speed: "
                + size / ((end + 1 - start)) + " Byte/millSec"); //142ms
*/

        String storePath1 = "../2982138_2995952";
        long start1 = System.currentTimeMillis();
        File file1 = new File(storePath1);
        FastAppender appender = new FastAppender(file1, 0, 110);

        cnt = 0;
        int curPos =0;
        while (cnt++ < lineNum) {
            System.out.println("line "+cnt+" position : "+appender.getCurrPos());
            appender.append(data);
            byte[] a = new byte[1];
            a[0] = '\n';
            appender.append(a);
        }
        appender.close();
        System.out.println("final position : "+appender.getCurrPos());
        long end1 = System.currentTimeMillis();
        System.out.println("\n Byte Buffer Writing time: " + (end1 - start1) + "ms, size: " + file1.length() + " Speed: "
                + size / ((end1 + 1 - start1)) + " Byte/millSec"); //377ms > output stream 185ms


    }

}
