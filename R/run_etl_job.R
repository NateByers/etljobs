run_etl_job <- function(job_location) {
  # job_location = "data-test/job1"

  implementation <- get_implementation(job_location)

  if(implementation == "in-memory") {
    j <- etl_job_in_memory$new(job_location)
  }

  j$add_parameters()
  j$add_source()
  j$add_filter()
  j$add_join()
  j$add_transform()
  j$add_summarize()
  j$add_load()

}

get_implementation <- function(location){
  # location <- "data-test/job1"
  params <- suppressWarnings(yaml.load_file(paste0(location, "/job.yaml")))
  if (!"implementation" %in% names(params)) {
    implementation <- "in-memory"
  } else {
    implementation <- params$implementation
  }
  return(implementation)
}
