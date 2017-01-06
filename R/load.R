load_csv_in_memory <- function(endpoint, fields, append){
  if(append) {
    headers <- names(read.csv(endpoint, nrows = 1))
    if(sum(!names(.self$output_table) %in% headers) > 0) {
      stop(paste("cannot append load data to table because column(s) do not match:",
                 names(.self$output_table)[!names(.self$output_table) %in% headers]))
    }
    for(i in headers){
      # i = headers[1]
      if(!i %in% names(.self$output_table)) {
        .self$output_table[[i]] <- NA
      }
    }
    .self$output_table <- .self$output_table[, headers]
  }
  write.table(.self$output_table, file = endpoint,
              append = append, row.names = FALSE,
              qmethod = "double", col.names = !append,
              sep = ",")
}


load_odbc_in_memory <- function(endpoint, table, append) {

  if(append) {
    #headers <-
  }
}
