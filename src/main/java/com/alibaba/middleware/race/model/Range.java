package com.alibaba.middleware.race.model;

import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

/**
 * Represent a range [a, b]
 */
public class Range implements Comparable {
    long minId;
    long maxId;
    //String dataPath; //data path is min+"_"+max

    public Range(long min, long max) {
        this.minId = min;
        this.maxId = max;
    }

    @Override
    public int compareTo(Object o) {
        return (int) (minId - ((Range) o).minId);
    }

    public static void main(String args[]){
        /*String n = "-2098979980";

        System.out.println("Long: "+ Long.parseLong(n));*/
        TreeMap<Long, String> treeMap = new TreeMap<Long, String>();
        treeMap.put(123l, "zhao");
        treeMap.put(234l, "li");
        treeMap.put(345l, "cao");
        for (Map.Entry<Long, String> entry : treeMap.entrySet()){
            System.out.println(entry.getKey()+" : "+ entry.getValue());
        }
        int size = treeMap.size();
        for (int i=0; i < size; i++){
            Map.Entry entry = treeMap.pollLastEntry();
            System.out.println(entry.getKey()+" : "+ entry.getValue());
        }

        for(long n : treeMap.descendingKeySet()){
            System.out.println(n + ": "+treeMap.get(n));
        }
    }
}