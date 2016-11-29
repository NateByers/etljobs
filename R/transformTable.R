#' Transform a \code{data.frame}
#'
#' @param table A \code{data.frame} that is a source to be transformed.
#' @param trans_table A \code{data.frame} with following columns: 'Field', 'Function_Name', and 'Order'.
#' See details below.
#' @param mapping_table A \code{data.frame} for mapping values character values.
#' @param attach Logical indicating if the new transformed data should be attached to the original source data.
#' @param verbose Do you want the deets?
#' @details The values in the \code{trans_table} indicate what column is being created, what function is being used
#' to create that column, and what order the columns will be created. The string under Function_Name needs to be
#' a function with two parameters: \code{table} and \code{mapping_table}. Those parameter values
#' will be passed on from the \code{transformTable} function. See the example below.
#' @examples
#' head(airquality)
#'
#' map <- data.frame(month_number = 1:12, month_name = month.name)
#'
#' makeMonth <- function(table, mapping_table){
#'   table <- left_join(table, mapping_table, by = c("Month" = "month_number"))
#'     return(table[["month_name"]])
#' }
#'
#' makeOzonePPM <- function(table, mapping_table){
#'   table <- table %>%
#'     mutate(ppm = Ozone/1000)
#'   return(table[["ppm"]])
#' }
#'
#' transformations <- read.table(header = TRUE, stringsAsFactors = FALSE, text = '
#' Field      Function_Name  Order
#' Month_Name makeMonth      2
#' Ozone_ppm  makeOzonePPM   1
#'                               ')
#' air <- transformTable(airquality, transformations, map)
#'
#' head(air)
#'
#' air <- transformTable(airquality, transformations, map, attach = FALSE)
#'
#' head(air)
#' @export
transformTable <- function(table, trans_table, mapping_table, attach = TRUE,
                           verbose = TRUE){
  # table <- airquality; trans_table <- transformations; mapping_table <- map; attach = TRUE; verbose = TRUE
  then <- Sys.time()
  for(i in 1:dim(trans_table)[1]){
    # i = 1
    trans_function = eval(parse(text = trans_table[trans_table[["Order"]] == as.numeric(i), "Function_Name"]))
    field = trans_table[trans_table[["Order"]] == as.numeric(i), "Field"]
    table[, field] <- trans_function(table, mapping_table)
  }
  if(verbose){
    print(Sys.time() - then)
  }
  if(!attach){
    table <- table[, trans_table[["Field"]]]
  }
  return(table)
}

