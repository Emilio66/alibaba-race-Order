# alibaba-race-Order
> Middleware challenging race

##Problem https://code.aliyun.com/MiddlewareRace/order-system


> Description:

There're lots of log files such as orders, buyers, goods transaction records were produced in Alibaba's daily transaction. The log file are usually large and their size are not fixed, probably large than main memory. Your task is to parsing this file, organize them in a resonable order then provide query interface for clients. The requirements are: 1. you have only one hour to parsing this file. 2. The query interface of your program will be call for 1 hour, the more queries you answered, the higher your rank will be.


> My idea:

I have tried to copy the source file and distribute them in a orgainzed way, but it doesn't work due time limit. Only 35G data was written out in my program. So I changed my mind, just build indices for every record right on the source file.

##Steps:

1. Parsing source data file concurrently, retrive the primary key of each row, distribute them to related bucket according to the key
2. Mataining each row's primary key with their offset in source data file, build association map for JOIN queries in buckets
3. When the buckets are almost full, flush the indices to disk
4. Waiting for completion of each buckets' indices building, then load part of data into memory for cache, utilize Direct Memory for cache partly due to the limitation of memory
5. Answer queries, analyze the query key to avoid unnecessary disk access, first look at the memory cache, if not hit, get the index bucket according to their key, find the <pk, offset> use hash, then retrive required rows in source data file according to the offset


