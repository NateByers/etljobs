context("sourceETLjob")
testHelpers()

j <- etljobs:::startETLjob("data-test/job1") %>%
  etljobs:::addETLsources() %>%
  etljobs:::addETLjoins()  %>%
  etljobs:::addETLtransformations() %>%
  etljobs:::addETLfilters() %>%
  etljobs:::addETLsummaries() %>%
  etljobs:::addETLreshape() %>%
  etljobs:::addETLload() %>%
  etljobs:::sourceETLjob()



