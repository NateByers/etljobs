addETLreshape <- function(etljob){
  # etljob <- j
  if(!"etljob_sources" %in% attr(etljob, "class")){
    stop("parameter object must have classes 'etljob_sources'")
  }
  job_location <- attr(etljob, "job_location")
  reshape_table <- read.csv(paste0(job_location, "/reshape.csv"),
                            stringsAsFactors = FALSE)
  attr(etljob, "reshape") <- reshape_table
  attr(etljob, "class") <- c("etljob_reshape", class(etljob))
  return(etljob)
}
