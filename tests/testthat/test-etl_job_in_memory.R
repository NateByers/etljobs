context("etl_job_in_memory")

j <- etljobs:::etl_job_in_memory$new("data-test/job1")
j$add_parameters()
j$add_source()
j$add_filter()
j$add_join()
j$add_transform()
j$add_summarize()
j$add_load()
j$source_data()

test_that("sources are in memory", {
  source_names <- sapply(j$source, function(x) x$source)
  source_table_names <- names(j$source_tables)
  expect_equal(sum(!source_table_names %in% source_names), 0)
})

test_that("if fields are specified, only those columns are in the data frames", {

})



