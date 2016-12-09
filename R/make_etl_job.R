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

  writeLines(c("name", "location", "type", "field", "field_type"),
    con = paste0(location, "/source.csv"), sep = ",")

  writeLines(c("source1_name", "source1_field", "source2_name",
               "source2_field", "type"),
    con = paste0(location, "/join.csv"), sep = ",")

  writeLines(c("new_field", "group_by", "transformation"),
    con = paste0(location, "/transform.csv"),sep = ",")

  writeLines(c("location", "type", "table", "append"),
    con = paste0(location, "/load.csv"), sep = ",")

  writeLines(c("field", "field_type", "code", "recode_value", "recode_field_type"),
             con = paste0(location, "/recode.csv"), sep = ",")

  writeLines(c("new_field", "group_by", "summarize"),
             con = paste0(location, "/summarize.csv"), sep = ",")

  writeLines(c("source", "group_by", "filter"),
             con = paste0(location, "/filter.csv"), sep = ",")

  writeLines(c("source", "type", "key", "value"),
             con = paste0(location, "/reshape.csv"), sep = ",")

  writeLines(as.yaml(list(language = "R")),
            con = paste0(location, "/job.yaml"))
}
