source_csv <- function(source_name, location, fields, types){
  # location <- source$location; fields <- source$fields$field; types <- source$fields$field_type
  columns <-strsplit(readLines(location, n = 1), ",")[[1]]
  fields[!grepl("\\S", fields)] <- NA

  # if there are no fields specified, just include all columns
  if(sum(!is.na(fields)) == 0) {
    column_types <- rep(NA, length(columns))
    fields <- columns
  } else {
    # get rid of NA fields and match up the field character types with the csv columns
    types <- types[!is.na(fields)]
    fields <- fields[!is.na(fields)]
    types[!grepl("\\S", types)] <- NA
    if(sum(!fields %in% columns) > 0) {
      stop(paste("fields missing in source", location))
    }
    names(types) <- fields
    column_types <- sapply(columns, function(x, types){
      if(x %in% names(types)) {
        type <- types[names(types) == x]
        names(type) <- NULL
        return(type)
      } else {
        return(as.character(NA))
      }
    }, types = types)
  }

  df <- read.csv(location, colClasses = column_types, stringsAsFactors = FALSE) %>%
    select_(.dots = fields)

  names(df) <- paste(source_name, names(df), sep = ".")

  return(df)
}
