package com.alibaba.middleware.race.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Created by zhaoz on 2016/7/31.
 */
public class RandomWriter {
    private MappedByteBuffer mappedBuffer;
    private FileChannel channel;
    private int startPos;  //this map zone's absolute starting point
    private long buffSize;
    private String filename;

    public RandomWriter(File file, int startPos, long buffSize){
        this.startPos = startPos;
        this.buffSize = buffSize;
        this.filename = file.getName();

        try {
            this.channel = new RandomAccessFile(file, "rw").getChannel();
            this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, startPos, buffSize);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * write long to absolute position in file
     * @param position
     * @param data
     */
    public void writeExactLong(int position, long data){
        //right on current map zone
        if(position >= startPos && (position - startPos)+8 < mappedBuffer.limit()){
            try {
                this.mappedBuffer.position(position - startPos);    //relative position in buffer
               // System.out.println("Hit in mapped buffer at "+position+", offset: "+mappedBuffer.position());

            }catch (IllegalArgumentException e){
                System.out.println("--- error Position: "+position+", Limit: "+mappedBuffer.limit());
            }

        }else{

            try {
                //remapping
                this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, position, buffSize);
                startPos = position;
                //System.out.println(filename+" random write remapping from "+ startPos);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //System.out.println("write: "+data+" at "+mappedBuffer.position()+", absolute: "+(startPos+mappedBuffer.position()));
        mappedBuffer.putLong(data);

    }

    /**
     * Append data to current buffer
     * @param data
     */
    public void writeLong(long data){
        if(mappedBuffer.remaining() < 8){
            try {
                startPos +=  mappedBuffer.position();   //new position
                mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, startPos, buffSize);
                //System.out.println(filename+" random write remapping from "+ startPos);

            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //System.out.println(" writing long at "+ mappedBuffer.position());
        mappedBuffer.putLong(data); //append to current position
    }

    public void writeExactly(int position, byte[] data){

        if( position >= startPos && (position - startPos) + data.length < mappedBuffer.limit()){
            try {
                this.mappedBuffer.position(position - startPos);    //update absolute starting position
                //System.out.println("Hit in mapped buffer at "+position+", offset: "+mappedBuffer.position());

            }catch (IllegalArgumentException e){
                System.out.println("--- error Position: "+position+", Limit: "+mappedBuffer.limit());
            }

        }else{
            //remapping

            try {
                this.mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, position, buffSize);
                this.startPos = position;   //update absolute starting position
                //System.out.println(filename+" random write remapping from "+ startPos);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        this.mappedBuffer.put(data);
    }

    public void write(byte[] data){
        //System.out.println("Put data[] at "+mappedBuffer.position()+" size: "+data.length);
        //not enough space, remapping
        if(mappedBuffer.remaining() < data.length){
            try {
                //mappedBuffer.compact();
                long size = (data.length > buffSize) ? data.length : buffSize;
                startPos += mappedBuffer.position();    //absolute position
                //System.out.println("write [] remapping from "+mappedBuffer.position()+", buffer size: "+size);
                mappedBuffer = channel.map(FileChannel.MapMode.READ_WRITE, startPos, size);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        mappedBuffer.put(data);
    }

    public void close(){
        try {
            //channel.truncate(channel.position());  //unsafe, affect mapping
            if(buffSize >= (10 << 20)) {
                FastAppender.unmap(mappedBuffer);
                System.out.println("clean buff size: "+buffSize);
            }
            mappedBuffer = null;
            channel.close();

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String args[]) {

        String output = "random";
        File file = new File(output);
        System.out.println("File length: "+file.length());
        RandomWriter writer = new RandomWriter(file, (int)file.length() +20, 1024);//start from 0

       /* writer.writeExactLong(0, 123);
        writer.writeExactLong(2, 123);
        writer.writeExactLong(1023, 123);
        writer.writeExactLong(1512, 123);*/
        byte[] dat = new byte[522];
        writer.write(dat);
        /*writer.writeLong(1024);
        writer.writeExactly(3024, dat);*/
        writer.close();
        System.out.println("File length: "+file.length());
    }
}
