addETLtransformations <- function(etljob){
  # etljob <- j
  if(sum(!c("etljob_joins", "etljob_sources") %in% attr(etljob, "class")) > 0){
    stop("parameter object must have classes 'etljob_joins' and 'etljob_sources'")
  }

  job_location <- attr(etljob, "job_location")
  transformations_table <- read.csv(paste0(job_location, "/transform.csv"),
                                    stringsAsFactors = FALSE)
  attr(etljob, "transformations") <- transformations_table
  attr(etljob, "class") <- c("etljob_transformations", class(etljob))
  return(etljob)
}
