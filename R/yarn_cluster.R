spark_yarn_cluster_get_property <- function(yarnSiteXml, rm) {
  xml2::xml_text(xml2::xml_find_all(
    yarnSiteXml,
    paste(
      "//name[.='",
      rm,
      "']/parent::property/value",
      sep = ""
    )
  ))
}

spark_yarn_cluster_get_gateway <- function() {
  confDir <- Sys.getenv("YARN_CONF_DIR")
  if (nchar(confDir) == 0) {

    # some systems don't set YARN_CONF_DIR but do set HADOOP_CONF_DIR
    confDir <- Sys.getenv("HADOOP_CONF_DIR")
    if (nchar(confDir) == 0) {
      stop("Yarn Cluster mode requires YARN_CONF_DIR or HADOOP_CONF_DIR to be set.")
    }
  }

  yarnSite <- file.path(confDir, "yarn-site.xml")
  if (!file.exists(yarnSite)) {
    stop("Yarn Cluster mode requires yarn-site.xml to exist under YARN_CONF_DIR")
  }

  yarnSiteXml <- xml2::read_xml(yarnSite)

  yarnResourceManagerHighAvailability <- spark_yarn_cluster_get_property(
    yarnSiteXml,
    "yarn.resourcemanager.ha.enabled"
  )

  if (length(yarnResourceManagerHighAvailability) > 0 &&
      grepl("TRUE", yarnResourceManagerHighAvailability, ignore.case = TRUE)) {

    rmIDs <- strsplit(spark_yarn_cluster_get_property(yarnSiteXml, "yarn.resourcemanager.ha.rm-ids"), ",")

    # http://<rm http address:port>/ws/v1/cluster/info

    mainResourceManager <- paste(
      "yarn.resourcemanager.address.",
      yarnResourceManagerHighAvailabilityId,
      sep = ""
    )
  }
  else {
    yarnResourceManagerAddress <- spark_yarn_cluster_get_property(yarnSite, "yarn.resourcemanager.address")
  }

  if (length(yarnResourceManagerAddress) == 0) {
    stop("Yarn Cluster mode uses `yarn.resourcemanager.address` but is not present in yarn-site.xml")
  }

  strsplit(yarnResourceManagerAddress, ":")[[1]][[1]]
}
