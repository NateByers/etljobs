addETLjoins <- function(etljob){
  # etljob <- j
  if(!"etljob_sources" %in% attr(etljob, "class")){
    stop("parameter object must have the class 'etljob_sources'")
  }
  job_location <- attr(etljob, "job_location")
  joins <- read.csv(paste0(job_location, "/join.csv"),
                            stringsAsFactors = FALSE)
  joins_stacked <- rbind(
    joins %>%
      select(source1_name, source1_field),
    joins %>%
      select(source2_name, source2_field) %>%
      rename(source1_name = source2_name, source1_field = source2_field)
    )

  apply(joins_stacked, 1, function(row, sources){
    name <- row[["source1_name"]]
    if(!name %in% names(sources)){
      stop(paste("source name", name, "in joins.csv file is not in source.csv file"))
    }
    field <- row[["source1_field"]]
    if(!field %in% sources[[name]]$fields$field){
      stop(paste("field", field, "is not in source.csv for source", name))
    }
  }, sources = attr(etljob, "sources"))

  attr(etljob, "joins") <- joins
  attr(etljob, "class") <- c("etljob_joins", class(etljob))

  return(etljob)
}
