context("etl_job_in_memory")

job_location <- "../../data-test"



output_location <- "../../data-test/output/"
source_location <- "../../data-test/sources/"


job1 <- read.csv(paste0(output_location, "job1.csv"), stringsAsFactors = FALSE)
subjects <- read.csv(paste0(source_location, "subjects.csv"))

test_that("job1 works", {
  expect_equal(names(job1), c("Subject_ID", "Sex", "Race"))
  
  expect_equal(
})



