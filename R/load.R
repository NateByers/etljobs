load_csv_in_memory <- function(df, endpoint, fields, append){
  if(append) {
    headers <- names(read.csv(endpoint, nrows = 1))
    if(sum(!names(df) %in% headers) > 0) {
      stop(paste("cannot append load data to table because column(s) do not match:",
                 names(df)[!names(df) %in% headers]))
    }
    for(i in headers){
      # i = headers[1]
      if(!i %in% names(df)) {
        df[[i]] <- NA
      }
    }
    df <- df[, headers]
  }
  write.table(df, file = endpoint,
              append = append, row.names = FALSE,
              qmethod = "double", col.names = !append,
              sep = ",")
}


load_odbc_in_memory <- function(df, endpoint, table, schema, append) {
  connection <- sapply(.self$connections, function(x) x$type == "odbc" & x$dsn == endpoint)
  
  if(sum(connection) != 1) etl_job_stop("load odbc endpoint must match exactly one connection")
  
  connection <- .self$connections[[which(connection == TRUE)]]$connection
  tables <- RODBC::sqlTables(connection)
  
  if(!table %in% tables[["TABLE_NAME"]]) etl_job_stop("load table not found in odbc endpoint")
  
  if(!is.na(schema)) table <- paste(schema, table, sep = ".")
  
  db_fields <- RODBC::sqlColumns(connection, table)[["COLUMN_NAME"]]
  output_columns <- names(df)
  
  if(sum(!output_columns %in% db_fields) != 0) etl_job_stop("some output columns do not match data base fields")
  
  for(i in db_fields) {
    if(!i %in% output_columns) {
      df[[i]] <- NA
    }
  }
  
  df <- df[, db_fields]
  
  RODBC::sqlSave(channel = connection, dat = df, tablename = table,
                 append = append, rownames = FALSE)
  
}
