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
delete_kudu_table <- function(sc,kudu_table){
  resp <- sc$kudu_context %>% invoke("deleteTable",kudu_table)
  exists("resp")
}
#' @export
kudu_insert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertRows",df,kudu_table)
  exists("resp")
}
#'@export
kudu_insert_ignore_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("insertIgnoreRows",df,kudu_table)
  exists("resp")
}
#'@export
kudu_upsert_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("upsertRows",df,kudu_table)
  exists("resp")
}
#'@export
kudu_update_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("updateRows",df,kudu_table)
  exists("resp")  
}
#'@export
kudu_delete_rows <- function(sc,df,kudu_table){
  resp <- get_kudu_context(sc) %>% invoke("deleteRows",df,kudu_table)
  exists("resp")  
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

