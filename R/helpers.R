process_join_table <- function(join_table) {

  join_table <- join_table %>%
    mutate(source_combo = paste(source1_name, source2_name),
           source1_field = paste(source1_name, source1_field, sep = "."),
           source2_field = paste(source2_name, source2_field, sep = "."))

  for(i in 1:dim(join_table)[1]) {
    # i = 2
    field <- join_table[i, "source1_field"]
    match_subset <- join_table %>%
      filter(source2_field == field)
        if(dim(match_subset)[1] == 1) {
          join_table[i, "source1_field"] <- match_subset[["source1_field"]]
        }
      }

  return(join_table)
}

process_char_columns <- function(df) {
  for(i in names(df)) {
    if(class(df[[i]]) == "character") {
      df[[i]] <- stringr::str_trim(df[[i]], "both")
      df[[i]][!grepl("\\S", df[[i]])] <- NA
    }
  }
  return(df)
}

etl_job_stop <- function(message) {
  odbcCloseAll()
  stop(message)
}

pass_parameters <- function(func, .dots) {
  # .self <- j; func <- "odbcConnect"
  parameters <- .self$parameters$pass_parameters[[func]]
  if(!is.null(parameters)) {
    for(i in names(parameters)) {
      .dots[[i]] <- parameters[[i]]
    }
  }
  function_text <- paste0(func, "(", names(.dots[[1]]), " = ", make_vector_text(.dots[[1]]))
  if(length(.dots) > 1) {
    for(i in names(.dots)) {
      # i = 1
      value <- parameters[[i]]
      value <- make_vector_text(value)
      function_text <- paste0(function_text, ", ", i, " = ", value)
    }
  }
  function_text <- paste0(function_text, ")")
}

make_vector_text <- function(vector) {
  # vector = c("this", "that")
  if(class(vector) == "character") {
    vector_text <- paste0("c('", vector[1], "'")
    if(length(vector) > 1) {
      for(i in vector[-1]) {
        vector_text <- paste0(vector_text, ", '", i, "'")
      }
    }
  } else {
    vector_text <- paste0("c(", vector[1])
    if(length(vector) > 1) {
      for(i in vector[-1]) {
        vector_text <- paste0(vector_text, ", ", i)
      }
    }
  }
  vector_text <- paste0(vector_text, ")")
  return(vector_text)
}
