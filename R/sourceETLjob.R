sourceETLjob <- function(etljob, supported_types = "csv"){
  # etljob <- j
  if(!"etljob_sources" %in% class(etljob)){
    stop("the parameter object must have the class 'etljob_sources'")
  }

  sources <- attr(etljob, "sources")

  types <- sapply(sources, function(x) x$type)

  if(sum(!types %in% supported_types) > 0){
    stop(paste("no support for source type(s)", types[!types %in% supported_types]))
  }

  tables <- lapply(sources, function(source){
    # source <- sources[[1]]
    if(source$type == "csv"){
      table <- sourceCSV(source$location, source$fields$field,
                         source$fields$field_type)
    }
    return(table)
  })

  etljob$source_tables <- tables

  return(etljob)

}

sourceCSV <- function(location, fields, types){
  # location <- source$location; fields <- source$fields$field; types <- source$fields$field_type
  columns <-strsplit(readLines(location, n = 1), ",")[[1]]
  if(sum(!fields %in% columns) > 0){
    stop(paste("fields missing in source", location))
  }

  names(types) <- fields
  df <- read.csv(location, colClasses = types) %>%
    select_(.dots = fields)

  return(df)
}
