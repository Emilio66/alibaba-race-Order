package com.alibaba.middleware.race.util;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
/*
测试环境: Windows10, 64bit, 4G mem, Intel i5 CPU

循环100万次写大约20几个字节, 最终结果:

D:/byteBufferWrite.tmp spent time=181毫秒
D:/byteBufferWrite_bigBuffer.tmp spent time=240毫秒 //bigger buffer size doesn't help
D:/mapmemWrite.tmp spent time=115毫秒
D:/streamWrite.tmp spent time=842毫秒
*/
//Java最快Append二进制内容到文件的方法
public class WriteSpeedTest {
	//值得注意的是：需要用二进制比较工具确认大家写的内容都是一样
	public static void main(String[] args) throws Exception {
		//没有缓冲的RandomAccessFile直接写, 因为耗时太长所以直接排除
        //randomAccessWrite("D:/test1.tmp",1000000);
		
		//用ByteBuffer做缓存的 RandomAccessFile写，在windows7 32bit环境测试，最快
        byteBufferWrite("D:/byteBufferWrite.tmp", 1000000,1024*150);
		
		//这个和上面一样，用的test2，但缓存size变大，测试证明缓存在大到一定程度后就没有提速的效果
        byteBufferWrite("D:/byteBufferWrite_bigBuffer.tmp", 1000000,1024*1024);
		
		//用MappedByteBuffer写，测试证明没有test2速度快，可能原因是MappedByteBuffer是内部用的AllowDirect
		//很奇怪，和Thinking in Java 4th 所说不同，用AllowDirect反而更慢，这点可在test2注释行有说明

		//而且MappedByteBuffer有个很重要的特性，只要map()文件就会先固定为和Map Size一样的大小，这不符合Append行为
        mapmemWrite("D:/mapmemWrite.tmp", 1000000, 1024*1024*20);
		
		//用带缓冲的一般流，实际证明这个比test2和test3还要慢		
        streamWrite("D:/streamWrite.tmp",1000000,1024*150);
	}

	public static void streamWrite(String file,int loop,int mapsize) throws Exception {
		//先将上次文件删除
		new File(file).delete();
		DataOutputStream byteBuffer = new DataOutputStream(
				new BufferedOutputStream(
						new FileOutputStream(new File(file),true),mapsize));


		byte[] b1 = new byte[]{'a','b','c','d','e','f','g','h'};
		byte[] utfstr = "this is a test".getBytes("UTF-8");
		long s = System.nanoTime();
		for(int i=0;i<loop;i++){
			byteBuffer.write(b1);
			byteBuffer.writeInt(i);
			byteBuffer.writeInt(i+1);
			
			byteBuffer.write(utfstr);
			byteBuffer.write((byte)'\n');
		}
		//因为close方法可能将缓冲中最后剩余的flush到文件, 所以要纳入计时
		byteBuffer.close();

		long d = System.nanoTime() - s;
		System.out.println(file+" spent time="+(d/1000000)+"毫秒");
	}
	
	
	public static void mapmemWrite(String file,int loop,int mapsize) throws Exception {
		//先将上次文件删除
		new File(file).delete();
		RandomAccessFile byteBuffer1 = new RandomAccessFile(file,"rw");

		FileChannel fc = byteBuffer1.getChannel();
		MappedByteBuffer byteBuffer = fc.map(MapMode.READ_WRITE, 0, mapsize);
		byteBuffer.clear();
		//在windows7 32bit 下, allocateDirect反而比allocate慢
		//ByteBuffer byteBuffer = ByteBuffer.allocateDirect(mapsize);
		
		byte[] b1 = new byte[]{'a','b','c','d','e','f','g','h'};
		byte[] utfstr = "this is a test".getBytes("UTF-8");
		long s = System.nanoTime();
		long count = 0;
		for(int i=0;i<loop;i++){
			if(byteBuffer.remaining()<140){
				System.out.println("remap");
				count += byteBuffer.position();  
		        byteBuffer = fc.map(MapMode.READ_WRITE, count, mapsize);
				//byteBuffer = fc.map(MapMode.READ_WRITE, byteBuffer.position(), byteBuffer.position()+mapsize); 
			}
			
			byteBuffer.put(b1);
			byteBuffer.putInt(i);
			byteBuffer.putInt(i+1);
			
			byteBuffer.put(utfstr);
			byteBuffer.put((byte)'\n');
		}
		//因为close方法可能将缓冲中最后剩余的flush到文件, 所以要纳入计时
		fc.close();
		byteBuffer1.close();

		long d = System.nanoTime() - s;
		System.out.println(file+" spent time="+(d/1000000)+"毫秒");
	}
	
	
	public static void byteBufferWrite(String file,int loop,int mapsize) throws Exception {
		//先将上次文件删除
		new File(file).delete();
		RandomAccessFile randomAccessFile = new RandomAccessFile(file,"rw");

		FileChannel fc = randomAccessFile.getChannel();
		ByteBuffer byteBuffer = ByteBuffer.allocate(mapsize);
		byteBuffer.clear();
		//在windows7 32bit 下, allocateDirect反而比allocate慢
		//ByteBuffer byteBuffer = ByteBuffer.allocateDirect(mapsize);
		
		byte[] b1 = new byte[]{'a','b','c','d','e','f','g','h'};
		byte[] utfstr = "this is a test".getBytes("UTF-8");
		long s = System.nanoTime();
		for(int i=0;i<loop;i++){
			byteBuffer.put(b1);
			byteBuffer.putInt(i);
			byteBuffer.putInt(i+1);
			
			byteBuffer.put(utfstr);
			byteBuffer.put((byte)'\n');
			
			if(byteBuffer.remaining()<140){
				byteBuffer.flip();
				fc.write(byteBuffer);
				byteBuffer.compact();
			}
		}
		byteBuffer.flip();
		fc.write(byteBuffer);
		//因为close方法可能将缓冲中最后剩余的flush到文件, 所以要纳入计时
		fc.close();
        randomAccessFile.close();

		long d = System.nanoTime() - s;
		System.out.println(file+" spent time="+(d/1000000)+"毫秒");
	}
	
	public static void randomAccessWrite(String file, int loop) throws Exception {
		new File(file).delete();
		RandomAccessFile byteBuffer = new RandomAccessFile(file,"rw");
		
		
		byte[] b1 = new byte[]{'a','b','c','d','e','f','g','h'};
		byte[] utfstr = "this is a test".getBytes("UTF-8");
		long s = System.nanoTime();
		for(int i=0;i<loop;i++){
			byteBuffer.write(b1);
			byteBuffer.writeInt(i);
			byteBuffer.writeInt(i+1);
			byteBuffer.write(utfstr);
			byteBuffer.write('\n');
		}
		//因为close方法可能将缓冲中最后剩余的flush到文件, 所以要纳入计时
		byteBuffer.close();

		long d = System.nanoTime() - s;
		System.out.println(file+" spent time="+(d/1000000)+"毫秒");
	}

}
