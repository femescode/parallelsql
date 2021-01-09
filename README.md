# parallelsql

#### 介绍
并行执行sql的工具，用于数据导出与在线统计。

#### 使用说明

1.  in查询导出csv
```bash
java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql "select * from order where (order_id,user_id) in (#{in})" --inFile "C:\infile.txt" --batchSize 10 -v -k -r -o temp.csv
```
2.  范围查询导出json
```bash
java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql "select * from order where add_time >= #{start} and add_time < #{end} limit 1" --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r -o temp.json
```
