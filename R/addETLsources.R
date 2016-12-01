addETLsources <- function(etljob){
  # etljob <- j
  if(attr(etljob, "class") != "etljob"){
    stop("parameter value must have class 'etljob'")
  }
  job_location <- attr(etljob, "job_location")
  source_table <- read.csv(paste0(job_location, "/source.csv"),
                            stringsAsFactors = FALSE)
  sources <- lapply(unique(source_table[["name"]]), function(source, table){
    table <- table %>%
      filter(name == source)

    location <- table %>%
      filter(!is.na(location), grepl("\\S", location)) %>%
      distinct()
    if(dim(location)[1] == 0){
      stop("please provide location in source.csv file")
    }
    if(dim(location)[1] > 1){
      stop("locations in source.csv file must be unique for a single source name")
    }
    location <- location[["location"]]

    type <- table %>%
      filter(!is.na(type), grepl("\\S", type)) %>%
      distinct()
    if(dim(type)[1] == 0){
      stop("please provide type in source.csv file")
    }
    if(dim(type)[1] > 1){
      stop("types in source.csv file must be unique for a single source name")
    }
    type <- type[["type"]]

    fields <- table %>%
      select(field, field_type) %>%
      distinct() %>%
      as.data.frame()

    types <- fields %>%
      filter(grepl("\\S", field_type))
    valid_types <- c("numeric", "integer", "character", "logical")
    if(sum(!types[["field_type"]] %in% valid_types) > 0){
      stop("invalid fiel_type in source.csv file")
    }

    etl_source <- list(source = source, location = location, type = type,
                       fields = fields)

    return(etl_source)

  }, table = source_table)

  names(sources) <- unique(source_table[["name"]])

  class(etljob) <- c("etljob_sources", "etljob")
  attr(etljob, "sources") <- sources

  return(etljob)
}
