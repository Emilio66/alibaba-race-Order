package com.alibaba.middleware.race.model;

/**
 * class for saving line & PK in input files
 */
public class Record implements Comparable {
    public Comparable key;
    public byte[] value;

    public Record(){}
    public Record(Comparable key, byte[] value) {
        this.key = key;
        this.value = value;
    }

    @Override
    public int compareTo(Object o) {
        return key.compareTo(((Record) o).key);
    }
}