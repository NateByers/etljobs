startETLjob <- function(job_location,
                        expected_files = c("source.csv", "join.csv",
                                           "transform.csv", "code.csv",
                                           "filter.csv", "summarize.csv",
                                           "reshape.csv", "load.csv", "job.yaml")){
  # job_location <- "test_job2"
  job_location <- sub("\\/$", "", job_location)

  if(!dir.exists(job_location)){
    stop("job directory does not exist")
  }

  files <- list.files(job_location)
  if(sum(!expected_files %in% files) > 0){
    stop(paste("some files are missing from the etl job directory:",
               expected_files[!expected_files %in% files]))
  }

  etljob <- list()

  attr(etljob, "class") <- "etljob"
  attr(etljob, "job_location") <- job_location

  return(etljob)
}
