package com.alibaba.middleware.race.util;

import com.alibaba.middleware.race.model.Record;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.TreeMap;

/**
 * thread for writing file quickly
 */
public class WriteThread implements Runnable {
    private File file;
    private byte[] data;

    public WriteThread(File file, byte[] data) {
        this.file = file;
        this.data = data;
    }

    public WriteThread(File file, TreeMap<Long, String> sortedMap) {
        this.file = file;
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        try {
            for (String record : sortedMap.values()) {
                byteStream.write(record.getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        this.data = byteStream.toByteArray();
    }

    public WriteThread(File file, List<Record> records) {
        this.file = file;
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream(records.size() << 7);
        for (Record record : records) {
            try {
                byteStream.write(record.value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        this.data = byteStream.toByteArray();
    }

    @Override
    public void run() {
        /*FastWriter writer = new FastWriter(file);
        writer.write(data);
        writer.close();*/
    }
}