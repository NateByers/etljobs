
run_etl_job("../../data-test/job1")

get_job <- function(job) {
  # job <- "job1"
  # root <- "data-test/"
  root <- "../../data-test/"
  files <- list.files(paste0(root, job))
  csv_files <- grep("\\.csv$", files, value = TRUE)
  csv_names <- stringr::str_split_fixed(csv_files, "\\.", 2)[, 1]
  tables <- lapply(csv_files, function(x) {
    read.csv(paste0(root, job, "/", x), stringsAsFactors = FALSE)
    })
  names(tables) <- csv_names
  tables[["job"]] <- yaml::yaml.load_file(paste0(root, job, "/job.yaml"))
}
