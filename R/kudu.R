# Kudu Sparklyr Extension
# Intended to provide access to Kudu tables in Sparklyr
#
#

#' @import sparklyr
#' @import magrittr

#' @export
kudu_context <- function(sc,kudu_master) {
  kc <- invoke_new(sc,"org.apache.kudu.spark.kudu.KuduContext",kudu_master)
  new_sc <- sc
  new_sc$kudu_context = kc
  new_sc$kudu_master = kudu_master
  new_sc
}

#' @export
read_kudu_table <- function(sc,kudu_table) {
  hc <- sc$hive_context
  kudu_master <- sc$kudu_master
  options <- list("kudu.master"=kudu_master,"kudu.table"=kudu_table)
  df <- hc %>%
    invoke("read") %>%
      invoke("format","org.apache.kudu.spark.kudu") %>%
        invoke("option","kudu.master",kudu_master) %>%
          invoke("option","kudu.table",kudu_table) %>%
            invoke("load")

  df
}

#' @export
kudu_table_exists <- function(sc,kudu_table){
  get_kudu_context(sc) %>% invoke("tableExists",kudu_table)
}
#' @export
create_kudu_table <- function(sc,table_name,schema,keys,options){
  get_kudu_context(sc) %>% invoke("createTable",table_name,schema,keys,options)

}
#' @export
delete_kudu_table <- function(sc,kudu_table){
  resp <- sc$kudu_context %>% invoke("deleteTable",kudu_table)
  exists("resp")
}
#' @export
get_kudu_table <- function(sc,kudu_table){
  tbl <- sc$kudu_context %>% invoke("syncClient") %>% invoke("openTable",kudu_table)
  tbl
}
#' @export
get_impala_ddl <- function(sc,kudu_table){
  master <- sc$kudu_master
  handler <- "com.cloudera.kudu.hive.KuduStorageHandler"
  tbl_name <- kudu_table %>% invoke("getName")
  keys <- c()
  schema_str <- ""

  columns <- kudu_table %>% invoke("getSchema") %>% invoke("getColumns") %>% invoke("toArray")
  for(col in columns){
    name <- col %>% invoke("getName")
    name_fmtd <- paste("`",name,"`",sep="")
    type <- col %>% invoke("getType") %>% invoke("getName")

    #Certain types are different between Kudu and Impala

    if(type == "int32"){
      type <- "int"
    }
    else if(type =='int64'){
      type <- "bigint"
    }
    else if(type =='int16'){
      type <- "smallint"
    }
    else if(type =="int8"){
      type <- "tinyint"
    }

    val <- paste(name_fmtd,type,sep=" ")

    if(col %>% invoke("isKey")){
      keys <- c(keys, name)
    }

    if(schema_str == ""){
      schema_str <- val
    }
    else{
      schema_str <- paste(schema_str,val,sep=",")
    }
  }
  keys_str <- paste(keys,collapse=",")
  ddl <- sprintf("CREATE EXTERNAL TABLE `%s` (%s) TBLPROPERTIES('storage_handler'='%s','kudu.table_name'='%s','kudu.master_addresses'='%s','kudu.key_columns'='%s');",tbl_name,schema_str,handler,tbl_name,master,keys_str)
  ddl
}
#' @export
kudu_insert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_insert_ignore_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertIgnoreRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_upsert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("upsertRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_update_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("updateRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
kudu_delete_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("deleteRows",spark_dataframe(df),kudu_table)
  exists("resp")
}
#'@export
sdf_schema <- function(df){
  spark_dataframe(df) %>% invoke("schema")
}

get_kudu_context <- function(sc){
  sc$kudu_context
}
add_sql_context <- function(sc){
  jsc <- sc$java_context
  sql_context <- invoke_static(
    sc,
    "org.apache.spark.sql.api.r.SQLUtils",
    "createSQLContext",
    jsc
  )
  sc$hive_context = sql_context
  sc
}

