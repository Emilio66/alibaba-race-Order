# alibaba-race-Order
> Middleware challenging race

##Problem https://code.aliyun.com/MiddlewareRace/order-system


> Description:

  In Alibaba's daily transactions, lots of log files such as orders, buyers, goods transaction records were generated. The log files are usually large and their size are not fixed, probably large than the main memory. Each record in the log file is a sequence of many key-value pairs separated by '\t'. 
  To simulate the problem and not expose customers' private information, we generate more than 100G **order** log files, more than 4G **goods** and **buyer** log files. There're many fields in each record, the required fields that must occur in log file are as follows: 
  
  
  **Order log file's minimum fields**
  
orderid - Long

buyerid - String

goodid - String

createtime - Long

 **Goods log file's minimum fields**
 
goodid - String

salerid - String

**Buyer log file's minimum fields**

buyerid - String


**Your task is to parsing these files, organize them in a resonable order then handle queries from clients correctly by implementing OrderSystemImpl class. The competing rules are:**

1. You have only one hour to parse and organize these log files. 
2. The queries will be issued for 1 hour, the more correct queries you answered, the higher your rank will be.

##Notice:##

There're 4 types of query:

1. queryOrder (based on orderid, return specified transaction records with required fields)
2. queryOrdersByBuyer，(based on buyerid and createtime，return all the transaction records of a buyer within required periods)
3. queryOrdersBySaler，(based on salerid and goodid，return all the transaction records of a saler's specific goods with required fields)
4. sumOrdersByGood，(based on goodid，return the sum of a specific field of goods)

*The required fields may exist in other log files, it means you probably need to JOIN other log file to get the correct answer*

*There're maybe query hotspot*

*The query programs are running concurrently, your program should supprot concurrent access*


##My idea:##

I have tried to copy the source log file and reorganize them on the local disks, but it can't be done in 1 hour, the maximum size of data that can be transfered to local disk is 35G with provided hardware conditions. So I changed my mind, I just save the indexes for every record on the source file and store these indexes to the local disks which are less than 10GB in total.

##Steps:

1. Parsing source data file concurrently, retrieving the primary key of each row, distributing them to corresponding buckets.
2. Recording each row's primary key and their offset in source data file, build association map for JOIN queries in buckets
3. When the buckets are almost full, flush the indexes to disks.
4. Waiting for completion of each buckets' indices building, then load part of data into memory for cache, utilize Direct Memory for cache partly due to the limitation of memory
5. Answer queries, analyze the query key to avoid unnecessary disk access, first look at the memory cache, if not hit, get the index bucket according to their key, find the <pk, offset> use hash, then retrieve required rows in source data file according to the offset

![architecture](architecture.jpg)
