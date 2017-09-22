spark_dependencies <- function(spark_version, scala_version, ...) {
  
  kudu_version <- getOption("kudu.version", default = "1.3.1")
  
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/kudu-spark_%s-%s-%s.jar", spark_version, scala_version, kudu_version),
        package = "kudusparklyr"
      )
    ),
    packages = c(
    )
  )
}

#' @import sparklyr
.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}