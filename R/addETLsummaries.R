addETLsummaries <- function(etljob){
  # etljob <- j
  if(!"etljob_sources" %in% attr(etljob, "class")){
    stop("parameter object must have classes 'etljob_sources'")
  }

  job_location <- attr(etljob, "job_location")
  summary_table <- read.csv(paste0(job_location, "/summarize.csv"),
                            stringsAsFactors = FALSE)
  attr(etljob, "summaries") <- summary_table
  attr(etljob, "class") <- c("etljob_filters", class(etljob))
  return(etljob)
}
