spark_dependencies <- function(spark_version, scala_version, ...) {
  sparklyr::spark_dependency(
    jars = c(
      system.file(
        sprintf("java/kudu-spark_%s-%s-1.3.1.jar", spark_version, scala_version),
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