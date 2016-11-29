createETLsources <- function(etl_job){
  sources_table <- read.csv(paste0(job_location, "/sources.csv"),
                            stringsAsFactors = FALSE)
  sources <- lapply(unique(sources_table[["name"]]), function(source, table){
    table <- table %>%
      filter(name == source)

    location <- table %>%
      filter(!is.na(location), grepl("\\S", location)) %>%
      distinct()
    if(dim(location)[1] == 0){
      stop("please provide location in sources.csv file")
    }
    if(dim(location)[1] > 1){
      stop("locations in sources.csv file must be unique for a single source name")
    }
    location <- location[["location"]]

    type <- table %>%
      filter(!is.na(type), grepl("\\S", type)) %>%
      distinct()
    if(dim(type)[1] == 0){
      stop("please provide type in sources.csv file")
    }
    if(dim(type)[1] > 1){
      stop("types in sources.csv file must be unique for a single source name")
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
      stop("invalid fiel_type in sources.csv file")
    }

    etl_source <- list(source = source, location = location, fields = fields)

    return(etl_source)

  }, table = sources_table)

  names(sources) <- unique(sources_table[["name"]])

  attr(sources, "class") <- "etljob_sources"
  attr(sources, "etl_job") <- job_location

  return(sources)
}
