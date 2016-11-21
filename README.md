####Kudusparklyr
###A Sparklyr extention for Kudu

#####Introduction
[Sparklyr](http://spark.rstudio.com/index.html) is an R interface for Apache Spark that is highly extensible. [Apache Kudu](http://kudu.apache.org/) is a new storage engine that enables fast analytics on fast data and fits use cases in the Hadoop ecosystem where you need to frequently update/delete data while also servicing analytical queries as it comes in (NRT). Kudusparklyr is a Sparklyr extension that leverages the [Kudu integration with Spark](https://github.com/cloudera/kudu/tree/master/java/kudu-spark) to make working with Kudu in an R environment easier. Usage requires Spark, Kudu, and Sparklyr and supports Spark 1.6 today. 

#####Installation
Install through devtools:
```R
library(devtools)
install_github("bkvarda/kudusparklyr")
```
Alternatively download the repo and load from local:
```R
library(devtools)
load_all('/path/to/kudusparklyr')
```

#####Functions
Create a KuduContext and append it to a sparklyr spark_connection object. 
```R
kudu_context(sc,kudu_master)
```
Read a Kudu table in as a Spark DataFrame
```R
read_kudu_table(sc,kudu_table_name)
```
Insert, insert(ignore), upsert, update, and delete rows from a table (based on the contents of a DataFrame):
```R
kudu_insert_rows(sc,data_frame,kudu_table)
kudu_insert_ignore_rows(sc,data_frame,kudu_table)
kudu_upsert_rows(sc,data_frame,kudu_table)
kudu_update_rows(sc,data_frame,kudu_table)
kudu_delete_rows(sc,data_frame,kudu_table)
```
Check if a Kudu table exists:
```R
kudu_table_exists(sc,kudu_table)
```
Delete a Kudu table:
```R
delete_kudu_table(sc,kudu_table)
```


#####Example
```R

library(devtools)
library(sparklyr)
library(DBI)
library(dplyr)
library(kudusparklyr)

#Initialize connection to Spark (on YARN)
sc <- spark_connect(master="yarn-client",version="1.6")

#Create a KuduContext for manipulating tables (writing, updating, etc). Appends reference to KuduContext and Kudu Master to your Spark connection object
sc <- kudu_context(sc,"ip-10-0-0-138.ec2.internal:7051")

#Read a Kudu table and create a Spark DataFrame
df <- read_kudu_table(sc, "particle_test")

#Read in a sample of your DataFrame
sdf_sample(df)

#Register as a table (needed for dplyr functionality as far as I can tell)
tbl <- sdf_register(df, "temp_tbl")

#Select only the 'coreid' column through the dplyr interface:
select(tbl,coreid)

#Select only the 'coreid' column through the DBI interface:
dbGetQuery(sc, "Select coreid FROM temp_tbl")

#Select the relative counts of each occurence of coreid through the DBI interface:
counts <- dbGetQuery(sc, "Select coreid, count(coreid) as count FROM temp_tbl GROUP BY coreid ORDER BY count DESC")

#Convert to a SparkDataframe
counts_df <- copy_to(sc,counts)

#Insert the contents of counts_df into "counts_table" - use spark_dataframe(counts_df) to get the Java reference of counts_df:
kudu_insert_rows(sc,spark_dataframe(counts_df),"counts_table")

#Read in the new Kudu table as Spark DF
df <- read_kudu_table(sc, "counts_table")

#Register as a temp table
counts_tbl <- sdf_register(df, "counts_tbl")

#Select specific records through the dplyr interface. This needs to end up being a list of keys to be deleted
records <- select(filter(counts_tbl, count <= 10),coreid)

#Delete these records from the counts_tbl table
kudu_delete_rows(sc,spark_dataframe(records),"counts_table")

```

#####Limitations

