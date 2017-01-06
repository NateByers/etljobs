#' Set credentials for an ODBC data source
#'
#' @details The credentials for the DSN are saved as environmental variables,
#' using the R function \code{Sys.setenv()}. The user name for the DSN is
#'
#'
#' @param dsn The name of the data source name
#' @param usr The user name for the connection
#' @param pwd The password for the connection
#'
#' @examples
#' set_odbc_credential("whiskey", "foo", "bar")
#' Sys.getenv(c("WHISKEY_USR", "WHISKEY_PWD"))
#' unset_odbc_credential("whiskey")
#' Sys.getenv(c("WHISKEY_USR", "WHISKEY_PWD"), unset = NA)
#' @export
set_odbc_credential <- function(dsn, usr, pwd) {
  # dsn <- "whiskey"; usr <- "foo"; pwd <- "bar"
  env_usr <- paste0(toupper(dsn), "_USR")
  env_pwd <- paste0(toupper(dsn), "_PWD")
  eval(parse(text = paste0("Sys.setenv(", env_usr, " = '", usr, "' , ",
                           env_pwd, " = '", pwd, "')")))
}

#' @describeIn set_odbc_credential Unset the ODBC credentials
#' @export
unset_odbc_credential <- function(dsn) {
  env_usr <- paste0(toupper(dsn), "_USR")
  env_pwd <- paste0(toupper(dsn), "_PWD")
  Sys.unsetenv(c(env_usr, env_pwd))
}

