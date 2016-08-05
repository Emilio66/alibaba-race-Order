package com.alibaba.middleware.race.util;

/**
 * Created by zhaoz on 2016/8/1.
 * retrieve interesting info from primitive type's bits
 */
public class BitHelper {

    //ORDER: FID_OFF: 0-31 offset, 32 - 40 file id
    public static long assembleOrderFidOff(byte fileId, long offset){

        long fId_off = (long) fileId;
        fId_off = fId_off << 32;
        fId_off = fId_off | offset;   //0-31 offset, 32 - 40 file id

        return fId_off;
    }

    /**
     * get order's store file id
     * @param fId_off
     * @return
     */
    public static byte getOrderFileId(long fId_off) {
        return (byte) (fId_off >>> 32);
    }

    /**
     * get order's offset in store file
     * @param fId_off
     * @return
     */
    public static long getOrderOffset(long fId_off) {

        long newValue = fId_off << 32;
        return newValue >>> 32;
    }

    //BUYER/GOOD FID_OFF: 0-28 offset, 29-31 file id (only 5 file, 000 - 101)
    public static int assembeGBFidOff(byte fileId, int offset){
        int fId_off = (int) fileId;
        fId_off = fId_off << 29;
        fId_off = fId_off | offset;

        return fId_off;
    }

    public static byte getGBFid(int fId_off){

        return (byte)(fId_off >>> 29); //logic move right
    }

    public static int getGBoff(int fId_off){
        return (fId_off & 0x0FFFFFFF);
    }

    public static void main(String args[]){
        int off = 0x0FFFFFFF;
        byte fid = 5;
        int f_of = assembeGBFidOff(fid, off);
        System.out.println("fid: "+ getGBFid(f_of)+" , off: "+ Integer.toBinaryString(getGBoff(f_of)));

        off = 123;
        fid = 4;
        f_of = assembeGBFidOff(fid, off);
        System.out.println("fid: "+ getGBFid(f_of)+" , off: "+ getGBoff(f_of));

        long offset = 232;
        fid = 43;
        long fid_off = assembleOrderFidOff(fid, offset);
        System.out.println("order fid: "+getOrderFileId(fid_off)+", off: "+getOrderOffset(fid_off));

        offset = Integer.MAX_VALUE;
        offset += Integer.MAX_VALUE;
        fid = 45;
        fid_off = assembleOrderFidOff(fid, offset);
        System.out.println("order fid: "+getOrderFileId(fid_off)+", off: "+getOrderOffset(fid_off));
    }
}
