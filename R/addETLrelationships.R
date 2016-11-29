addETLrelationships <- function(etl_sources){
  # etl_sources <- j
  if(attr(etl_sources, "class") != "etljob_sources"){
    stop("parameter must have the class 'etljob_sources'")
  }
  job_location <- attr(etl_sources, "etl_job")
  relationships <- read.csv(paste0(job_location, "/relationships.csv"),
                            stringsAsFactors = FALSE)
  relationships_stacked <- rbind(
    relationships %>%
      select(source1_name, source1_field),
    relationships %>%
      select(source2_name, source2_field) %>%
      rename(source1_name = source2_name, source1_field = source2_field)
    )

  apply(relationships_stacked, 1, function(row, sources){
    name <- row[["source1_name"]]
    if(!name %in% names(sources)){
      stop(paste("source name", name, "in relationships.csv file is not in sources.csv file"))
    }
    field <- row[["source1_field"]]
    if(!field %in% sources[[name]]$fields$field){
      stop(paste("field", field, "is not in sources.csv for source", name))
    }
  }, sources = etl_sources)

  attr(etl_sources, "relationships") <- relationships

  return(etl_sources)
}
