***Enviroments***:

OS: Ubuntu 18.04 LTS [1]
Hadoop-2.6.5 [2]

[1] https://www.ubuntu.com/download/server
[2] http://ftp.jaist.ac.jp/pub/apache/hadoop/common/hadoop-2.6.5/hadoop-2.6.5.tar.gz

build: ***mvn clean install***

***Task1 - unique pages in a session***
***session interval***: 15 minutes
***MapReduce job***: org.noname.paypay.session.SessionByIpTask
***Mapper***: extract ip, timestamp as millisecond, url as page
***Reducer***: collect ip, {start_time: max_timestamp, end_time: min_timestamp, pages: list(url)}
***usage: bin/hadoop jar ${jar_file} org.noname.paypay.session.SessionByIpTask ${hdfs_input_folder} ${hdfs_output_folder}***

***Task2 - engaged Ips***
***Note: Read text from Task1 output file***
***MapReduce job***: org.noname.paypay.ip.EngagedIpTask
***Mapper***: extract start_time, end_time, and ip, then shuffle <(end_time - start_time), ip>
***Reducer***: collect ip as a list while having same time duration(end_time - start_time).
***SortComparator***: reverse the time duration(end_time - start_time) key.
***usage: bin/hadoop jar ${jar_file} org.noname.paypay.ip.EngagedIpTask ${hdfs_input_folder} ${hdfs_output_folder}***