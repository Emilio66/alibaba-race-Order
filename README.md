# alibaba-race-Order
> Middleware challenging race

##Problem https://code.aliyun.com/MiddlewareRace/order-system


> Description:

  At Alibaba's, there're millions transaction records were generated everyday, they were saved in different log files according to the record type, such as orders, buyers, goods, etc. The log files are usually very large and their size are not fixed, sometimes large than the main memory. Each record in the log file is a sequence in a row consists of multiple key-value pairs separated by '\t'. 
  To simulate the problem and not expose customers' privacy, we generate more than 100G **order** log files, more than 4G **goods** and **buyer** log files. There're many fields in each record, the fields that must occur in log file are as follows: 
  
  
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


**Your task is implementing OrderSystemImpl class which is able to parse these files, organize them in a resonable order that can handle queries from clients correctly. The competition rules are:**

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

At begining, I have tried to copy the source log file from the server and distribute them to the local disks in a reasonable order, but I found that this can't be done in 1 hour, the maximum size of data that can be transfered to local disks in 1 hour is 35G with the provided hardware environment. So I changed my thought, instead of reorganize all the data to local disks, I build the index for every record in the original file and just store these indexes to the local disks. The size of all index files are less than 10GB in total, so the solution is feasible.

##Steps:

1. Parsing source data file concurrently, retrieving the primary key of each row, distributing the key and the record's offset in file to corresponding buckets.
2. Constructing the index for each row by using the primary key and its offset in original log file, building association table for JOIN queries which may require fields in different types of log files.
3. When the buckets are almost full, flush the indexes in memory to disks.
4. When completing the index construction, we load part of the hot data into memory for data cache in a LRU manner, utilize Direct Memory as the mechanism of cache becuase of the limitation of memory
5. Answering queries, analyzing the query key, first look at the memory cache to avoid unnecessary disk access, if not hit, get the index bucket according to their key, find the <pk, offset> use hashing, then find the required rows in original data file with the record's offset in file.

![architecture](architecture.jpg)
