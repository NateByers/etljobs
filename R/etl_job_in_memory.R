etl_job_in_memory <- setRefClass("etl_job_in_memory",
                                 contains = "etl_job",
                                 fields = c("sources", "merged"))

etl_job_in_memory$methods(
  source_data = function(supported_types = "csv"){
    # sources <- j$source
    sources <- .self$source
    types <- sapply(sources, function(x) x$type)

    if(sum(!types %in% supported_types) > 0){
      stop(paste("no support for source type(s)", types[!types %in% supported_types]))
    }

    tables <- lapply(sources, function(source){
      # source <- sources[[5]]
      if(source$type == "csv"){
        table <- source_csv(source$location, source$fields$field,
                            source$fields$field_type)
      }
      return(table)
    })

    .self$sources <- tables

  }

)
