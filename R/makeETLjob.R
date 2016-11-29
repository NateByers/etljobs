makeETLjob <- function(job_location,
                       expected_files = c("sources.csv", "relationships.csv",
                                          "transformations.csv", "code.csv",
                                          "summarize.csv", "reshape.csv",
                                          "load.csv", "parameters")){
  job_location <- sub("\\/$", "", job_location)
}
