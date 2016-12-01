joinETLjob <- function(etljob){
  # etljob <- j
  if(!"source_tables" %in% names(etljob)){
    stop("you need data to join")
  }
  job_location <- attr(etljob, "job_location")
  for(i in names(etljob$source_tables)){
    names(etljob$source_tables[[i]]) <- paste(i, names(etljob$source_tables[[i]]),
                                              sep = ".")
  }

  join_table <- read.csv(paste0(job_location, "/join.csv"),
                         stringsAsFactors = FALSE)

  joins <- join_table %>%
    select(source1_name, source2_name, type)

  combined_table <- data.frame()
  for(j in 1:dim(joins)[1]){
    join_by_table <- join_table %>%
      filter(source1_name == joins[j, "source1_name"],
             source2_name == joins[j, "source2_name"])
    join_by <- paste(joins[j, "source2_name"], join_by_table[["source2_field"]],
                     sep = ".")
    names(join_by) <- paste(joins[j, "source1_name"], join_by_table[["source1_field"]],
                            sep = ".")
    join_function <- eval(parse(text = paste0(joins[j, "type"], "_join")))
    combined_table <- join_function(etljob$source_tables[[joins[j, "source1_name"]]],
                                    etljob$source_tables[[joins[j, "source2_name"]]],
                                    join_by)
  }

  etljob[["source_tables"]] <- NULL
  etljob[["combined_table"]] <- combined_table
  return(etljob)

}



determineJoinFunction <- function(type){
  my_function <- eval(parse(text = "rnorm"))

}


