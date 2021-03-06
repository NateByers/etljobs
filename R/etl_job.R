etl_job <- setRefClass("etl_job",
                       fields = c("job_location", "parameters", "connect",
                                  "source", "filter", "join", "transform",
                                  "recode", "reshape", "summarize", "load"))

etl_job$methods(
  initialize = function(location,
                        expected_files = c("source.csv", "join.csv",
                                           "transform.csv", "recode.csv",
                                           "filter.csv", "summarize.csv",
                                           "reshape.csv", "load.csv",
                                           "job.yaml")){
    # location <- "test_job2"
    location <- sub("\\/$", "", location)
    
    if(!dir.exists(location)){
      stop("job directory does not exist")
    }
    
    files <- list.files(location)
    if(sum(!expected_files %in% files) > 0){
      stop(paste("some files are missing from the etl job directory:",
                 expected_files[!expected_files %in% files]))
    }
    
    .self$job_location <- location
    
  }
)

etl_job$methods(
  add_parameters = function(params) {
    
    if (params$language[1] != "R") {
      stop("job yaml file does not indicate R as the programming language")
    }
    
    .self$parameters <- params
    
  }
)

etl_job$methods(
  add_connect = function() {
    connect_table <- read.csv(paste0(.self$job_location, "/connect.csv"),
                              stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$connect <- connect_table
  }
)


etl_job$methods(
  add_source = function() {
    # .self <- j
    source_table <- read.csv(paste0(.self$job_location, "/source.csv"),
                             stringsAsFactors = FALSE) %>%
      process_char_columns()
    
    if(dim(source_table)[1] <1 ) stop("you need sources")
    
    source_names <- unique(source_table[["name"]])[!is.na(unique(source_table[["name"]]))]
    
    sources <- lapply(source_names, function(source, table){
      # source <- source_names[1]; table <- source_table
      
      table <- table %>%
        dplyr::filter(name == source)
      
      location <- table %>%
        dplyr::filter(!is.na(location), grepl("\\S", location)) %>%
        dplyr::select(location) %>%
        dplyr::distinct()
      if(dim(location)[1] == 0){
        stop("please provide location in source.csv file")
      }
      if(dim(location)[1] > 1){
        stop("locations in source.csv file must be unique for a single source name")
      }
      location <- location[["location"]]
      
      type <- table %>%
        dplyr::filter(!is.na(type), grepl("\\S", type)) %>%
        dplyr::select(type) %>%
        dplyr::distinct()
      if(dim(type)[1] == 0){
        stop("please provide type in source.csv file")
      }
      if(dim(type)[1] > 1){
        stop("types in source.csv file must be unique for a single source name")
      }
      type <- type[["type"]]
      
      fields <- table %>%
        dplyr::select(field, field_type) %>%
        dplyr::distinct() %>%
        as.data.frame()
      
      types <- fields %>%
        dplyr::filter(grepl("\\S", field_type))
      valid_types <- c("numeric", "integer", "character", "logical")
      if(sum(!types[["field_type"]] %in% valid_types) > 0){
        stop("invalid fiel_type in source.csv file")
      }
      
      etl_source <- list(source = source, location = location, type = type,
                         fields = fields)
      
      return(etl_source)
      
    }, table = source_table)
    
    names(sources) <- unique(source_table[["name"]])
    
    .self$source <- sources
    
  }
  
)

etl_job$methods(
  add_filter = function() {
    # job_location <- "../reconciliation_etl/ADNI_D"
    filter_table <- read.csv(paste0(.self$job_location, "/filter.csv"),
                             stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$filter <- filter_table
  }
)

etl_job$methods(
  add_recode = function() {
    # job_location <- "../reconciliation_etl/ADNI_D"
    recode_table <- read.csv(paste0(.self$job_location, "/recode.csv"),
                             colClasses = "character",
                             stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$recode <- recode_table
  }
)

etl_job$methods(
  add_join = function() {
    
    # job_location <- j$job_location; source <- j$source
    joins <- read.csv(paste0(.self$job_location, "/join.csv"),
                      stringsAsFactors = FALSE) %>%
      process_char_columns()
    
    if(dim(joins)[1] > 0) {
      joins_stacked <- rbind(
        joins %>%
          dplyr::select(source1_name, source1_field),
        joins %>%
          dplyr::select(source2_name, source2_field) %>%
          dplyr::rename(source1_name = source2_name, source1_field = source2_field)
      )
      
      apply(joins_stacked, 1, function(row, sources){
        name <- row[["source1_name"]]
        if(!name %in% names(sources)){
          stop(paste("source name", name, "in joins.csv file is not in source.csv file"))
        }
      }, sources = source)
    }
    .self$join <- joins
  }
)

etl_job$methods(
  add_transform = function() {
    
    transformations_table <- read.csv(paste0(.self$job_location, "/transform.csv"),
                                      stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$transform <- transformations_table
    
    if(dim(transformations_table)[1] == 0) {
      stop("must have at least one record in the transform.csv file")
    }
    
  }
)

etl_job$methods(
  add_summarize = function() {
    summary_table <- read.csv(paste0(.self$job_location, "/summarize.csv"),
                              stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$summarize <- summary_table
  }
)

etl_job$methods(
  add_reshape = function() {
    reshape_table <- read.csv(paste0(.self$job_location, "/reshape.csv"),
                              stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$reshape <- reshape_table
  }
)

etl_job$methods(
  add_code = function() {
    code_table <- read.csv(paste0(.self$job_location, "/code.csv"),
                           stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$code <- code_table
  }
)

etl_job$methods(
  add_load = function(){
    load_table <- read.csv(paste0(.self$job_location, "/load.csv"),
                           stringsAsFactors = FALSE) %>%
      process_char_columns()
    .self$load <- load_table
  }
)

