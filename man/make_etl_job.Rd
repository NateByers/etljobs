% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/make_etl_job.R
\name{make_etl_job}
\alias{make_etl_job}
\title{Make an etl job by creating a directory with supporting documents}
\usage{
make_etl_job(location = ".", name)
}
\arguments{
\item{location}{Location where the new directory will be}

\item{name}{The name of the new directory}
}
\description{
Make an etl job by creating a directory with supporting documents
}
\details{
This function will create a new directory with the following files
    in it: source.csv, join.csv, transform.csv, filter.csv,
    recode.csv, summarize.csv, reshape.csv, load.csv, and job.yaml.
}
\examples{
makeETLjob(name = "my_new_job")
}

