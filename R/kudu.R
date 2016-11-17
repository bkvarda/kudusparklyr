# Hello, world! - From Spark and Scala
#
# This is an example package which compiles
# a hello function in scala and deploys it
# to spark using sparklyr.
#
# You can learn more about sparklyr at:
#
#   http://spark.rstudio.com/
#

#' @import sparklyr
#' @import magrittr

#' @export
kudu_context <- function(sc,kudu_master) {
  kc <- invoke_new(sc, "org.apache.kudu.spark.kudu.KuduContext", kudu_master)
  kc
}

#' @export
read_kudu_table <- function(sc,kudu_master,kudu_table) {
  hc <- hive_context(sc)
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
kudu_table_exists <- function(kc,kudu_table){
  kc %>% invoke("tableExists",kudu_table)
}

#' @export
delete_kudu_table <- function(kc,kudu_table){
   resp <- kc %>% invoke("deleteTable",kudu_table)
   exists("resp")
}
#' @export
kudu_insert_rows <- function(kc,df,kudu_table){
  kc %>% invoke("insertRows",df,kudu_table)

}
