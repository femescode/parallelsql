# parallelsql

#### Description
parallel execute sql tools, to export to file and more。

#### Instructions

1.  in query to export csv file
```bash
java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql "select * from order where (order_id,user_id) in (#{in})" --inFile "C:\infile.txt" --batchSize 10 -v -k -r -o temp.csv
```
2.  range query to export json file
```bash
java -jar target/parallelsql.jar -hlocalhost -P3306 -uroot -pxxxx -Dshop --sql "select * from order where add_time >= #{start} and add_time < #{end} limit 1" --rangeStart 1610087881 --rangeEnd 1610141407 --rangeSpan 10000 -v -k -r -o temp.json
```
