package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.file.FileParser;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class FastReader {
    private static final byte LINE_END = '\n';
    private MappedByteBuffer byteBuffer = null;
    private FileChannel fileChannel = null;
    private File file;
    private long size;
    private long startPos;

    public FastReader(File file) {
        this(file, 0, file.length());
    }

    public FastReader(File file, long startPos, long len) {
        this.file = file;
        this.startPos = startPos;
        this.size = len;
        prepare(startPos, size);
    }

    //friendly for random access, seek the given starting position for reading
    public boolean prepare(long startPos, long len) {
        try {
            this.fileChannel = new FileInputStream(this.file).getChannel();
            this.size = len;

            if((startPos + len) > file.length())
                this.size = file.length() - startPos;

            this.byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, startPos, size);
            this.byteBuffer.load();
            this.byteBuffer.position(0);//back to the start of this memory block
            //System.out.println("buffer size: "+byteBuffer.capacity()+" limit: "+byteBuffer.limit());

            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public byte[] exactLine(long linePos) {

        try {

            byteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, linePos, 4096);//map 1 page
            byteBuffer.load();
            byteBuffer.position(0);//back to the start of this memory block

            return nextLine();

        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    public byte[] nextLine() {
        //System.out.println("buffer poistion: "+byteBuffer.position()+" limit: "+byteBuffer.limit());
        if (byteBuffer.hasRemaining()) {
            byte b;
            boolean isCompleteLine = true;
            ByteArrayOutputStream byteStream = new ByteArrayOutputStream(FileParser.LINE_SIZE);//we got a large line for efficiency

            b = byteBuffer.get();

            try {
                while (true) {

                    if(size <= 0){   //end of file
                        isCompleteLine = false; //split file doesn't have a line end in the end of file
                        break;
                    }

                    byteStream.write(b);    //write byte

                    if (b == LINE_END) {   //it requires every line has a '\n', use it as a end of line
                        break;

                    } else if (byteBuffer.hasRemaining()) {   //end of buffer
                        b = byteBuffer.get();

                    }else {
                        //sliding the buffer window forward until a complete line is read
                        long pos = byteBuffer.position();
                        startPos += pos;

                        if((startPos + size) > file.length()) {
                            size = file.length() - startPos;
                        }

                        if(size <= 0){   //end of file
                            isCompleteLine = false;
                            break;
                        }

                        byteBuffer = this.fileChannel.map(FileChannel.MapMode.READ_ONLY, startPos, size);
                        byteBuffer.load();
                        byteBuffer.position(0);
                        b = byteBuffer.get();
                        //System.out.println("Resize start position: "+startPos+" limit: "+byteBuffer.limit());
                    }
                }

            } catch (IOException e) {
                e.printStackTrace();
            }

             if (isCompleteLine)
                 return byteStream.toByteArray();
             else
                 return null;    //incomplete line, happens in split file

        }

        return null;//end of file, not a complete line
    }

    public long getLong(){
        if(byteBuffer.remaining() > 8)
            return byteBuffer.getLong();
        else
            return -1;
    }

    public long getCurrPos() {
        return startPos + byteBuffer.position();
    }


    public boolean close() {
        try {
            //maybe costly for small chunk reading, need to be check
            if(size > (2 << 20) )
                FastAppender.unmap(byteBuffer); //free direct memory after reading large chunk
            byteBuffer = null;  //release object in JVM
            fileChannel.close();
            return true;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    public static void main(String[] args) {
        File file = new File("buyer_records.txt");
        FastReader reader = new FastReader(file,0,4096);
        FastWriter writer = new FastWriter(new File("test.txt"),file.length());
        byte[] data;
        while((data = reader.nextLine())!= null) {
           // System.out.println(new String(data));
            writer.write(data);
        }
        reader.close();
        writer.close();
    }

}