% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/PmcaCovariate.R
\name{getPMCACovariateData}
\alias{getPMCACovariateData}
\title{Extracts PMCA value}
\usage{
getPMCACovariateData(
  connection,
  oracleTempSchema = NULL,
  cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = "#cohort_person",
  rowIdField = "row_id",
  aggregated,
  cohortId,
  covariateSettings
)
}
\arguments{
\item{connection}{The database connection}

\item{oracleTempSchema}{The temp schema if using oracle}

\item{cdmDatabaseSchema}{The schema of the OMOP CDM data}

\item{cdmVersion}{version of the OMOP CDM data}

\item{cohortTable}{the table name that contains the target population cohort}

\item{rowIdField}{string representing the unique identifier in the target population cohort}

\item{aggregated}{whether the covariate should be aggregated}

\item{cohortId}{cohort id for the target population cohort}

\item{covariateSettings}{settings for the covariate cohorts and time periods}
}
\value{
The PMCA covariate will now be added to the data
}
\description{
Extracts PMCA value
}
\details{
The user specifies a cohort and the PMCA algorithm is calculated for each patient in the cohort
}
