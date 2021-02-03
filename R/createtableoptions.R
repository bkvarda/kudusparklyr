#' @title kudu_table_options
#' @description invoke kudu_table_options
#' 
#' @param sc spark connection
#'
#' @import magrittr
#' @export
kudu_table_options <- function(sc){
  opts <- invoke_new(sc,
                     "org.apache.kudu.client.CreateTableOptions")
  opts
}

#' @title set_num_replicas
#' @description set number of replicas
#' 
#' @param opts options
#' @param num_replicas integer containing the number of replicas
#' @export
set_num_replicas <- function(opts, num_replicas){
  opts %>% invoke("setNumReplicas", as.integer(num_replicas))
}

#' @title add_hash_partitions
#' @description add hash partitions to kudu table
#' 
#' @param opts additional options
#' @param columns columns for partitioning
#' @param buckets buckets
#' @param seed seed
#'
#' @export
add_hash_partitions <- function(opts, columns, buckets,
                                seed = 0){
  cols <- invoke_new(sc, "java.util.ArrayList")
  ### probably sc can be exchanged by opts
  ### or spark_connection(opts)? NOTE no visible binding ...
  for(item in columns){
    cols %>% invoke("add", item)
  }
  opts %>% invoke("addHashPartitions", cols, as.integer(buckets),
                  as.integer(seed))
}


#' @title set_range_partition_columns
#' @description set range partition columns for kudu table
#' 
#' @param opts additional options
#' @param columns columns
#'
#' @export
set_range_partition_columns <- function(opts, columns){
  cols <- invoke_new(sc, "java.util.ArrayList")
  ### probably sc can be exchanged by opts
  ### or spark_connection(opts)? NOTE no visible binding ...
  for(item in columns){
    cols %>% invoke("add", item)
  }
  opts %>% invoke("setRangePartitionColumns", columns)
}
