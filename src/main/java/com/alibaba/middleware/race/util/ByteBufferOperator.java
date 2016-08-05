package com.alibaba.middleware.race.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhaoz on 2016/7/31.
 */
public class ByteBufferOperator {
    private ByteBuffer byteBuffer;
    private FileChannel channel;
    private long buffSize;
    private long maxPos;    //as file length
    private boolean isWrite = false;
    //default read mode
    public ByteBufferOperator(File file, long startPos, int buffSize) {
        this.buffSize = buffSize;

        try {
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            channel.position(startPos);
            byteBuffer = ByteBuffer.allocate(buffSize); //read or write buffer
            channel.read(byteBuffer);
            byteBuffer.flip();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void remap(int newSize){
        byteBuffer = ByteBuffer.allocate(newSize); //read or write buffer
        try {
            channel.read(byteBuffer);
        } catch (IOException e) {
            e.printStackTrace();
        }
        byteBuffer.flip();
    }

    public ByteBufferOperator(File file, long startPos, int buffSize, boolean isWrite) {
        this.buffSize = buffSize;
        this.isWrite = true;

        try {
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            channel.position(startPos);
            byteBuffer = ByteBuffer.allocate(buffSize); //read or write buffer

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public long readLong() {
        //System.out.println(" Reading long, position: "+byteBuffer.position()+", remaining "+
        //byteBuffer.remaining()+", file position: "+channel.position());
        if (byteBuffer.hasRemaining())
            return byteBuffer.getLong();

        return -1;

    }

    public boolean seek(long position) {
        try {
            this.channel.position(position);
            byteBuffer.clear(); //clear this one
            channel.read(byteBuffer);
            byteBuffer.flip();

        } catch (IOException e) {

            return false;
        }

        return true;
    }

    public void writeLong(long data) {

        try {
            if (byteBuffer.remaining() < 8) {
                byteBuffer.flip();
                channel.write(byteBuffer);
                byteBuffer.compact();
                byteBuffer.clear(); //clear data;
            }

            byteBuffer.putLong(data);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void writeExactLong(long position, long data) {
        //whether position in current buffer zone
        try {
            long curPos = channel.position();
            //if in buffer [position,limit]
            if ((position < curPos && position > (curPos - byteBuffer.position())) ||
                    position > curPos && position < (curPos + byteBuffer.remaining() - 8)) {

                byteBuffer.putLong(data);

            } else {

                //flush first
                byteBuffer.flip();
                channel.write(byteBuffer);
                channel.position(position);
                byteBuffer.clear();
                byteBuffer.putLong(data);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * clear buffer, flush remaining data to disk
     */
    public void close() {
        if (isWrite && byteBuffer.position() != 0) {
            byteBuffer.flip();
            try {
                channel.write(byteBuffer);
                channel.truncate(channel.position());   //cut extra space
                channel.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String ags[]) {
        File file = new File("byte");
        ByteBufferOperator operator = new ByteBufferOperator(file, 0, 128, true);
        operator.writeLong(123);
        operator.writeLong(234);
        operator.writeLong(334);
        operator.writeLong(434);
        operator.writeLong(534);
        operator.writeLong(634);
        operator.close();
        operator = new ByteBufferOperator(file, 0, 128);
        operator.seek(8);   //read next long
        long n;
        int i=0;
        while ((n = operator.readLong()) != -1) {
            System.out.println("read " + n);
            operator.seek((i++) * 16);
        }
    }
}
