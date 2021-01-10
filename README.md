# parallelsql

#### 介绍
并行执行sql的工具，用于数据导出与在线统计。

#### 使用说明

1.  in查询导出csv
```bash
$ cat infile.csv
order_id,user_id
1315179275010002,1160013421
1410139275010001,3150019087

$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select * from order where (order_id,user_id) in (#{in})" \
    --inFile "C:\infile.csv" --batchSize 10 -v -k -r -o temp.csv
```
2.  范围查询导出json
```bash
$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select * from order where add_time >= #{start} and add_time < #{end} limit 1" \
    --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r -o temp.json
```
3.  范围查询多个结果group by聚合
```bash
$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select user_id,count(*) num from order where add_time >= #{start} and add_time < #{end} group by user_id" \
    --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r --collector agg -o temp.json
```
