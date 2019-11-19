# Intended to provide access to Kudu tables in Sparklyr
#
#' @import sparklyr
#' @import magrittr

#' @title kudu_context
#' @description creates kc kudu context
#' 
#' @param sc spark connection
#' @param kudu_master kudu master
#'
#' @export
kudu_context <- function(sc, kudu_master) {
  kc <- invoke_new(sc, "org.apache.kudu.spark.kudu.KuduContext",
                   kudu_master, spark_context(sc))
  new_sc <- sc
  new_sc$kudu_context <- kc
  new_sc$kudu_master <- kudu_master
  new_sc
}

#' @title read_kudu_table
#' @description reads a kudu table into spark
#' 
#' @param sc spark connection
#' @param kudu_table name of kudu table
#'
#' @examples
#' \dontrun{
#' table <- read_kudu_table(sc, "table_name")
#' }
#' 
#' @export
read_kudu_table <- function(sc, kudu_table) {
  hc <- hive_context(sc)
  kudu_master <- sc$kudu_master
  options <- list("kudu.master" = kudu_master,
                  "kudu.table" = kudu_table)
  df <- hc %>%
    invoke("read") %>%
      invoke("format", "org.apache.kudu.spark.kudu") %>%
        invoke("option", "kudu.master", kudu_master) %>%
          invoke("option", "kudu.table", kudu_table) %>%
            invoke("load")

  df
}

#' @title kudu_table_exists
#' @description checks if a kudu table exists
#' 
#' @param sc spark connection
#' @param kudu_table name of kudu table
#' 
#' @examples
#' \dontrun{
#' kudu_table_exists(sc, "table_name")
#' }
#'
#' @export
kudu_table_exists <- function(sc, kudu_table){
  get_kudu_context(sc) %>% invoke("tableExists", kudu_table)
}

#' @title create_kudu_table
#' @description creates a kudu table
#' 
#' @param sc spark connection
#' @param table_name table name
#' @param schema database in hive 
#' @param keys key columns
#' @param options additional options 
#'
#' @export
create_kudu_table <- function(sc, table_name, schema, keys,
                              options){
  get_kudu_context(sc) %>% invoke("createTable", table_name,
                                  schema, keys, options)
}

#' @title delete_kudu_table
#' @description deletes a kudu table
#' 
#' @param sc spark connection
#'
#' @param kudu_table table name
#'
#' @export
delete_kudu_table <- function(sc, kudu_table){
  resp <- sc$kudu_context %>% invoke("deleteTable", kudu_table)
  resp
}

#' @title get_kudu_table
#' @description gets a kudu table
#' 
#' @param sc spark connection
#' @param kudu_table kudu table name
#'
#' @export
get_kudu_table <- function(sc, kudu_table){
  tbl <- sc$kudu_context %>% invoke("syncClient") %>%
    invoke("openTable", kudu_table)
  tbl
}

#' @title get_impala_ddl
#' @description builds impala sql for table creation
#' 
#' @param sc spark connection
#' @param kudu_table kudu table name
#' @param beta set to TRUE if using beta Impala-Kudu
#'        (default = FALSE)
#'
#' @examples
#' \dontrun{
#' table_dll <- get_impala_ddl(sc, "table_name")
#' table_ddl <- get_impala_ddl(sc, "table_name", beta = TRUE)
#' }
#'
#' @export
get_impala_ddl <- function(sc, kudu_table, beta = FALSE){
  master <- sc$kudu_master
  handler <- "com.cloudera.kudu.hive.KuduStorageHandler"
  tbl_name <- kudu_table %>% invoke("getName")
  keys <- c()
  schema_str <- ""
  
  if(beta){
  columns <- kudu_table %>% invoke("getSchema") %>%
    invoke("getColumns") %>% invoke("toArray")
  for(col in columns){
    name <- col %>% invoke("getName")
    name_fmtd <- paste("`", name, "`", sep = "")
    type <- col %>% invoke("getType") %>% invoke("getName")

    ## Certain types are different between Kudu and Impala
  
    if(type == "int32"){
      type <- "int"
    }
    else if(type == 'int64'){
      type <- "bigint"
    }
    else if(type == 'int16'){
      type <- "smallint"
    }
    else if(type == "int8"){
      type <- "tinyint"
    }

    val <- paste(name_fmtd, type, sep = " ")

    if(col %>% invoke("isKey")){
      keys <- c(keys, name)
    }

    if(schema_str == ""){
      schema_str <- val
    }
    else{
      schema_str <- paste(schema_str, val, sep = ",")
    }
  }
  keys_str <- paste(keys, collapse = ",")
  ddl <- sprintf("CREATE EXTERNAL TABLE `%s` (%s) TBLPROPERTIES('storage_handler'='%s','kudu.table_name'='%s','kudu.master_addresses'='%s','kudu.key_columns'='%s');", tbl_name, schema_str, handler, tbl_name, master, keys_str)
  }
  else{
  ddl <- sprintf("CREATE EXTERNAL TABLE `%s` STORED AS KUDU TBLPROPERTIES('kudu.table_name'='%s','kudu.master_addresses'='%s');", tbl_name, tbl_name, master)
  }
  return(ddl)
}

#' @title kudu_insert_rows
#' @description inserts rows into a kudu table
#' 
#' @param sc spark connection
#' @param df spark dataframe
#' @param kudu_table table name
#'
#' @export
kudu_insert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>%
    invoke("insertRows", spark_dataframe(df), kudu_table)
  resp
}

#' @title kudu_insert_ignore_rows
#' @description inserts rows into a kudu table
#' 
#' @param sc spark connection
#' @param df spark dataframe
#' @param kudu_table table name
#'
#' @export
kudu_insert_ignore_rows <- function(sc, df, kudu_table){
  resp <- get_kudu_context(sc) %>%
    invoke("insertIgnoreRows", spark_dataframe(df), kudu_table)
  resp
}

#' @title kudu_upsert_rows
#' @description upserts rows into a kudu table
#' 
#' @param sc spark connection
#' @param df spark dataframe
#' @param kudu_table table name
#'
#' @export
kudu_upsert_rows <- function(sc, df, kudu_table){
  resp <- get_kudu_context(sc) %>%
    invoke("upsertRows", spark_dataframe(df), kudu_table)
  resp
}

#' @title kudu_update_rows
#' @description updates rows into a kudu table
#' 
#' @param sc spark connection
#' @param df spark dataframe
#' @param kudu_table table name
#'
#' @export
kudu_update_rows <- function(sc, df, kudu_table){
  resp <- get_kudu_context(sc) %>%
    invoke("updateRows", spark_dataframe(df), kudu_table)
  resp
}

#' @title kudu_delete_rows
#' @description deletes rows into a kudu table
#' 
#' @param sc spark connection
#' @param df spark dataframe
#' @param kudu_table table name
#'
#' @export
kudu_delete_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>%
    invoke("deleteRows", spark_dataframe(df), kudu_table)
  resp
}

#' @title sdf_head
#' @description show first row of dataframe content
#' 
#' @param df dataframe
#'
#' @export
sdf_head <- function(df){
  spark_dataframe(df) %>% invoke("head")
}

#' @title sdf_schema
#' @description return the java representation of the df schema
#' 
#' @param df dataframe
#'
#' @export
sdf_schema <- function(df){
  spark_dataframe(df) %>% invoke("schema")
}

#' @title get_kudu_context
#'
#' @param sc spark connection
#'
#' @return kudu context object
#' @export
get_kudu_context <- function(sc){
  sc$kudu_context
}

#' @title add_sql_context
#'
#' @param sc spark connection
#'
#' @return spark connection
#' @export
add_sql_context <- function(sc){
  jsc <- sc$java_context
  sql_context <- invoke_static(
    sc, "org.apache.spark.sql.api.r.SQLUtils",
    "createSQLContext", jsc)
  sc$hive_context <- sql_context
  sc
 }

