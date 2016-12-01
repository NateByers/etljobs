addETLload <- function(etljob){
  # etljob <- j
  if(!"etljob_sources" %in% attr(etljob, "class")){
    stop("parameter object must have classes 'etljob_sources'")
  }
  job_location <- attr(etljob, "job_location")
  load_table <- read.csv(paste0(job_location, "/load.csv"),
                         stringsAsFactors = FALSE)
  attr(etljob, "load") <- load_table
  attr(etljob, "class") <- c("etljob_reshape", class(etljob))
  return(etljob)
}
