### Kudusparklyr
#### A Sparklyr extention for Kudu

It's a fork from https://github.com/bkvarda/kudusparklyr, because
it's not actively maintained anymore and I needed newer versions
(i.e. jars).

#### Introduction
[Sparklyr](https://spark.rstudio.com/index.html) is an R interface for Apache Spark that is highly extensible. [Apache Kudu](https://kudu.apache.org/) is a new storage engine that enables fast analytics on fast data and fits use cases in the Hadoop ecosystem where you need to frequently update/delete data while also servicing analytical queries as it comes in (NRT). Kudusparklyr is a Sparklyr extension that leverages the [Kudu integration with Spark](https://github.com/cloudera/kudu/tree/master/java/kudu-spark) to make working with Kudu in an R environment easier. Usage requires Spark, Kudu, and Sparklyr and supports Spark 2.x today. Spark 1.6 support has been deprecated, and may no longer work.

#### Installation
Install through devtools:
```R
remotes::install_github("nachti/kudusparklyr")
```
Alternatively download the repo and load from local:
```R
pkgload::load_all('/path/to/kudusparklyr')
```

#### Specifying Kudu version
The Kudu/Spark integration is made available through JARs that come as part of this package. Currently, the only JARs that are made part of the project are those that are requested by people that use this package - so if you need a new version, let me know. The default Kudu version that is loaded is 1.3.1 in order to maintain backwards compatibility for users - this may change eventually. In order to use the JAR for a specific version of Kudu, you will need to set options before you create the spark connection:

```
library(kudusparklyr)
options(kudu.version = "1.4.0")
sc <- spark_connect(master="yarn-client",version="2.1", config = list(default = list(spark.yarn.keytab="/home/ec2-user/navi.keytab",spark.yarn.principal="navi@CLOUDERA.INTERNAL")))
```
Support for Spark 1.6 is deprecated by the kudu/spark integration package that this depends on, so support for Spark 1.6 is also deprecated here as well. 

#### Functions
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
Create a Kudu Options object (needed for creating a table):
```R
kudu_table_options(sc)
```
Changing options properties (partition_columns need to be a list() of columns):
```R
set_num_replicas(options,num_replicas)
add_hash_partitions(options,parition_columns,num_buckets,seed=0)
set_range_partition_columns(options,partition_columns)
```
Or more elegantly using magrittr pipes:
```R
options <- kudu_table_options(sc) %>% set_num_replicas(3) %>% add_hash_partitions(list("playerID"),16)
```
Extract the schema of a dataframe (can be used in non-kudu scenarios):
```R
sdf_schema(df)
```
Create a Kudu table:
```R
create_kudu_table(sc,tbl_name,schema,key_columns,kudu_options)

#Example:
create_kudu_table(sc, 'batting_table',schema,list("playerID","yearID","teamID"),options)
```
Get a kudu table object
```R
get_kudu_table(sc,tbl_name)
```
Get Impala DDL for a table
```R
get_impala_ddl(sc,tbl)

#Example 
tbl <- get_kudu_table(sc,"particle_test")
get_impala_ddl(sc,tbl)
```

#### Example
```R

library(devtools)
library(sparklyr)
library(DBI)
library(dplyr)
library(kudusparklyr)
library(magrittr)

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

#Copy the Batting table into Spark as a DF:
batting_tbl <- copy_to(sc, Lahman::Batting, "batting")

#Sample the DF:
sdf_sample(batting_tbl)

#Let's create a Kudu table. We'll derive a schema from the DataFrame and set some required options for our table...

#Derive the schema:
schema <- sdf_schema(batting_tbl)

#Create a Kudu options object which is needed to create a table. We can use magrittr pipes to make this easier:
options <- kudu_table_options(sc) %>% set_num_replicas(3) %>% add_hash_partitions(list("playerID"),16)

#Create the table:
create_kudu_table(sc, 'batting_table',schema,list("playerID","yearID","teamID"),options)

#Insert rows (this particular dataset has a bunch of duplicate rows):
kudu_insert_ignore_rows(sc,batting_tbl,'batting_table')

#Read the new table, register it and filter it with a magrittr pipe:
df <- read_kudu_table(sc, 'batting_table') %>% sdf_register('batting_temp') %>% filter(yearID > 1990)

#Grab a sample (with ouput as shown in console):
sdf_sample(df)
Source:   query [?? x 22]
Database: spark connection master=yarn-client app=sparklyr local=FALSE

    playerID yearID teamID stint  lgID     G    AB     R     H   X2B   X3B    HR   RBI    SB    CS    BB    SO   IBB
       <chr>  <int>  <chr> <int> <chr> <int> <int> <int> <int> <int> <int> <int> <int> <int> <int> <int> <int> <int>
1  alfonan01   1997    FLO     1    NL    17     3     0     0     0     0     0     0     0     0     0     3     0
2  alfonan01   1997    FLO     1    NL    17     3     0     0     0     0     0     0     0     0     0     3     0
3  alfonan01   1998    FLO     1    NL    58     4     0     0     0     0     0     0     0     0     0     2     0
4  alfonan01   1999    FLO     1    NL    73     2     0     0     0     0     0     0     0     0     0     2     0
5  alfonan01   1999    FLO     1    NL    73     2     0     0     0     0     0     0     0     0     0     2     0
6  alfonan01   1999    FLO     1    NL    73     2     0     0     0     0     0     0     0     0     0     2     0
7  alfonan01   1999    FLO     1    NL    73     2     0     0     0     0     0     0     0     0     0     2     0
8  alfonan01   1999    FLO     1    NL    73     2     0     0     0     0     0     0     0     0     0     2     0
9  alfonan01   2000    FLO     1    NL    68     0     0     0     0     0     0     0     0     0     0     0     0
10 alfonan01   2000    FLO     1    NL    68     0     0     0     0     0     0     0     0     0     0     0     0
 ... with more rows, and 4 more variables: HBP <int>, SH <int>, SF <int>, GIDP <int>

#Delete the table 
delete_kudu_table(sc, 'batting_table')

#Validate:
kudu_table_exists(sc, 'batting_table')
[1] FALSE

```

#### Limitations
This package relies on Kudu/Spark integration that is found here (https://github.com/cloudera/kudu/tree/master/java/kudu-spark).

The necessary jars are downloaded from
https://mvnrepository.com/artifact/org.apache.kudu/kudu-spark2
and renamed manually due to the convention
`sprintf("java/kudu-spark_%s-%s-%s.jar", spark_version,
scala_version, kudu_version)`
although the spark_version ist more or less irrelevant. It's
stored in the `inst/java` directory of this package then.
