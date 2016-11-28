
#' @import magrittr
#' @export
kudu_table_options <- function(sc){
  opts <- invoke_new(sc,"org.apache.kudu.client.CreateTableOptions")
  opts
}
#' @export
set_num_replicas <- function(opts,num_replicas){
  opts %>% invoke("setNumReplicas",as.integer(num_replicas))
}
#' @export
add_hash_partitions <- function(opts,columns,buckets,seed = 0){
  cols <- invoke_new(sc,"java.util.ArrayList")
  for(item in columns){
    cols %>% invoke("add",item)
  }
  opts %>% invoke("addHashPartitions",cols,as.integer(buckets),as.integer(seed))
}
#' @export
set_range_partition_columns <- function(opts,columns){
  for(item in columns){
    cols %>% invoke("add",item)
  }
  opts %>% invoke("setRangePartitionColumns",columns)
}
