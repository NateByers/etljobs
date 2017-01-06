#' Make an etl job by creating a directory with supporting documents
#'
#' @param location Location where the new directory will be
#' @param name The name of the new directory
#' @details This function will create a new directory with the following files
#'     in it: source.csv, join.csv, transform.csv, filter.csv,
#'     recode.csv, summarize.csv, reshape.csv, load.csv, and job.yaml.
#' @examples
#' makeETLjob(name = "my_new_job")
#' @export
make_etl_job <- function(location = ".", name){
  location <- paste(location, name, sep = "/")
  if(dir.exists(location)){
    stop("job already exists")
  }
  dir.create(location)

  writeLines(paste("type", "name", "username", "password", sep = ","),
             con = paste0(location, "/connect.csv"))

  writeLines(paste("name", "location", "type", "table", "field", "field_type",
                   sep = ","), con = paste0(location, "/source.csv"))

  writeLines(paste("source1_name", "source1_field", "source2_name",
                   "source2_field", "type", sep = ","),
             con = paste0(location, "/join.csv"))

  writeLines(paste("new_field", "group_by", "transformation", sep = ","),
             con = paste0(location, "/transform.csv"))

  writeLines(paste("endpoint", "type", "table", "fields", "append",
                   sep = ","), con = paste0(location, "/load.csv"))

  writeLines(paste("field", "code", "recode_value", sep = ","),
             con = paste0(location, "/recode.csv"))

  writeLines(paste("new_field", "group_by", "summarize", sep = ","),
             con = paste0(location, "/summarize.csv"))

  writeLines(paste("source", "group_by", "filter", sep = ","),
             con = paste0(location, "/filter.csv"))

  writeLines(paste("source", "type", "key", "value", sep = ","),
             con = paste0(location, "/reshape.csv"))

  writeLines(as.yaml(list(language = "R")),
            con = paste0(location, "/job.yaml"))
}
