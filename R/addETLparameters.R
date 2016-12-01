addETLparameters <- function(etljob){
  if(!"etljob" %in% class(etljob)){
    stop("the parameter object must have the class 'etljob'")
  }

  job_location <- attr(etljob, "job_location")
  parameters <- yaml.load_file(paste0(job_location, "/job.yaml"))

  if(parameters$language[1] != "R"){
    stop("job yaml file does not indicate R as the programming language")
  }

  if(!"order" %in% names(parameters)){
    parameters$order <- c("source", "filter", "join", "transform", "code",
                          "summarize", "reshape", "load")
  }

  if(parameters$order[1] != "source"){
    stop("'source' must be first in the order parameter")
  }

  if(last(parameters$order) != "load"){
    stop("'load' must be the last in the order parameter")
  }

  attr(etljob, "order") <- parameters$order

  return(etljob)
}
