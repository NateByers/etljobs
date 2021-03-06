#' Run an etl job
#'
#' @param location Location of job directory
#' @examples
#' run_etl_job("my_new_job")
#' @export
run_etl_job <- function(job_location) {
  # job_location = "data-test/job1"

  params <- suppressWarnings(yaml::yaml.load_file(paste0(job_location, "/job.yaml")))
  if (!"implementation" %in% names(params)) {
    implementation <- "in-memory"
  } else {
    implementation <- params$implementation
  }
  
  if(implementation == "in-memory") {
    j <- etl_job_in_memory$new(job_location)
  }

  j$add_parameters(params)
  j$add_source()
  j$add_filter()
  j$add_recode()
  j$add_join()
  j$add_transform()
  j$add_summarize()
  j$add_reshape()
  j$add_load()
  j$source_data()
  j$filter_sources()
  j$recode_sources()
  j$reshape_sources()
  j$join_tables()
  j$transform_table()
  j$filter_output()
  j$summarize_output()
  j$reshape_output()
  j$load_data()

}

