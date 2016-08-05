package com.alibaba.middleware.race.util;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.security.AccessController;
import java.security.PrivilegedAction;

/**
 * Created by zhaoz on 2016/7/20.
 */
public class FastAppender {
    private FileChannel channel;
    private MappedByteBuffer mappedBuffer;
    private long currPos;   //文件关闭后对象就销毁，因此追加可以在外部控制或者将文件尾部特殊字符覆盖
    private long buffSize;
    private String filename;

    /**
     * create a file appender by using MappedByteBuffer
     * @param file
     * @param bufferSize
     */
    public FastAppender(File file, long curpos, long bufferSize){
        try {
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.filename = file.getName();
            this.currPos = curpos;
            this.buffSize = bufferSize;
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, currPos, bufferSize);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public boolean seek(int pos){
        //if large than current limit, remap
        return false;
    }

    public void append(byte[] data){
        //not enough space, remapping
        if(mappedBuffer.remaining() < data.length){
            try {
                //mappedBuffer.compact();
                mappedBuffer.force();
                mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, currPos, data.length+buffSize);
                System.out.println(filename+" remapping from "+currPos+", buffer size: "+mappedBuffer.capacity());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        mappedBuffer.put(data);
        currPos += data.length;
    }

    public void appendLong(long data){

        if(mappedBuffer.remaining() < 8){
            try {
                //mappedBuffer.force();
                mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, currPos, buffSize);
                System.out.println(filename+" remapping from "+currPos+", buffer size: "+mappedBuffer.capacity());
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        mappedBuffer.putLong(data);
        currPos += 8;
    }

    public long getCurrPos(){
        return currPos;
    }
    public void close(){
        try {
            //mappedBuffer.compact(); //discard the vacant space after the valid content
            //channel.truncate(currPos);  //cut the crap, good for sequential write
            if(buffSize > (5 << 20))
                unmap(mappedBuffer);
            mappedBuffer = null;
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 在MappedByteBuffer释放后再对它进行读操作的话就会引发jvm crash，在并发情况下很容易发生
     * 正在释放时另一个线程正开始读取，于是crash就发生了。所以为了系统稳定性释放前一般需要检
     * 查是否还有线程在读或写
     * @param mappedByteBuffer
     */
    public static void unmap(final MappedByteBuffer mappedByteBuffer) {
        try {
            if (mappedByteBuffer == null) {
                return;
            }

            mappedByteBuffer.force();
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                @Override
                @SuppressWarnings("restriction")
                public Object run() {
                    try {
                        Method getCleanerMethod = mappedByteBuffer.getClass()
                                .getMethod("cleaner", new Class[0]);
                        getCleanerMethod.setAccessible(true);
                        sun.misc.Cleaner cleaner =
                                (sun.misc.Cleaner) getCleanerMethod
                                        .invoke(mappedByteBuffer, new Object[0]);
                        cleaner.clean();

                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                    System.out.println("clean MappedByteBuffer completed");
                    return null;
                }
            });

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public MappedByteBuffer getMappedBuffer(){
        return  this.mappedBuffer;
    }
    //take out the meaningful bit
    public static int getFileId(long file_offset){
        return (int) (file_offset >> 32); //32~44th bit
    }
    public static int getOffset(long file_offset){
        return (int) file_offset; //0~31th bit
    }

    public static void main(String args[]){
        //writing several lines, check whether they're right
        int line_num = 4;
        String output= "append.txt";
        FastReader reader = new FastReader(new File("order_records.txt"));
        FastAppender appender = new FastAppender(new File(output),0,1024);//start from 0

        long pos = appender.getCurrPos();
        for(int i =0 ; i<line_num; i++){
            String line = new String(reader.nextLine());
            System.out.println(i+" line "+line);
            appender.append(line.getBytes());
        }
        pos +=appender.getCurrPos();//read 4 line
        System.out.println("Current pos: "+pos);
        unmap(appender.getMappedBuffer());
        reader.close();
        appender.close();

//        reader = new FastReader(new File("order_records.txt"), pos, 1024);
//        appender = new FastAppender(new File(output),pos,1024);
//
//        for(int i =0 ; i<line_num; i++){
//            String line = new String(reader.nextLine());
//            System.out.println(i+" line "+line);
//            appender.append(line.getBytes());   //write another 4 lines
//        }
//
//
//        File out = new File(output);
//        reader = new FastReader(out, pos, out.length()-pos);
//        for(int i =0 ; i<line_num; i++){
//            byte[] data = reader.nextLine();    //read last 4 lines
//            if(data != null) {
//                String line = new String(data);
//                System.out.println(i + " line " + line);
//            }else {
//                System.out.println("EOF");
//                break;
//            }
//        }
        reader.close();
        /*long file_offset = 0L;
        int fileId = 2048;
        int offset = Integer.MAX_VALUE;
        file_offset = fileId;
        System.out.println("file_offset: "+file_offset);

        file_offset = (file_offset << 32) ;

        System.out.println("file_offset: "+file_offset);
        file_offset |= offset;
        System.out.println("file_offset: "+file_offset+", fileId "+getFileId(file_offset)+"  , offset: "+getOffset(file_offset));
        */}
}
