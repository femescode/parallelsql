# parallelsql

#### Description
parallel execute sql tools, to export to file and more。

#### Instructions

1.  in query to export csv file
```bash
$ cat infile.csv
order_id,user_id
1315179275010002,1160013421
1410139275010001,3150019087

$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select * from order where (order_id,user_id) in (#{in})" \
    --inFile "C:\infile.csv" --batchSize 10 -v -k -r -o temp.csv
```
2.  range query to export json file
```bash
$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select * from order where add_time >= #{start} and add_time < #{end} limit 1" \
    --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r -o temp.json
```
3.  range query and group by multi result
```bash
$ java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop \
    --sql "select user_id,count(*) num from order where add_time >= #{start} and add_time < #{end} group by user_id" \
    --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r --collector agg -o temp.json
```
