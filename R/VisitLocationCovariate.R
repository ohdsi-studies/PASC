#' Extracts PMCA value
#'
#' @details
#' Creates a covariate specifying whether the visit that overlaps with the target cohort index date is outpatient or not
#'
#' @param connection  The database connection
#' @param oracleTempSchema  The temp schema if using oracle
#' @param cdmDatabaseSchema  The schema of the OMOP CDM data
#' @param cdmVersion  version of the OMOP CDM data
#' @param cohortTable  the table name that contains the target population cohort
#' @param rowIdField  string representing the unique identifier in the target population cohort
#' @param aggregated  whether the covariate should be aggregated
#' @param cohortId  cohort id for the target population cohort
#' @param covariateSettings  settings for the covariate cohorts and time periods
#'
#' @return
#' The VisitLocation covariate will now be added to the data
#'
#' @export
getVisitLocationCovariateData <- function(
  connection,
  oracleTempSchema = NULL,
  cdmDatabaseSchema,
  cdmVersion = "5",
  cohortTable = "#cohort_person",
  rowIdField = "row_id",
  aggregated,
  cohortId,
  covariateSettings
){
  
  # Some SQL to construct the covariate:
  sql <- paste(
    "select distinct a.@row_id_field AS row_id, ",
    " @analysis_id as covariate_id,",
    " 1 as covariate_value",
    "from @cohort_temp_table a inner join  ",
    "@cdm_database_schema.visit_occurrence visit",
    " on a.subject_id = visit.person_id",
    " and a.cohort_start_date <= visit.visit_end_date ",
    " and a.cohort_end_date >= visit.visit_start_date",
    " where visit_concept_id in (9202,8756,8947,5084);
    "
  )
  
  sql <- SqlRender::render(
    sql,
    cohort_temp_table = cohortTable,
    row_id_field = rowIdField,
    cdm_database_schema = cdmDatabaseSchema,
    analysis_id = covariateSettings$analysisId
  )
  
  sql <- SqlRender::translate(
    sql = sql, 
    targetDialect = attr(connection, "dbms"),
    tempEmulationSchema = oracleTempSchema
  )
  
  # Retrieve the covariate:
  covariates <- DatabaseConnector::querySql(connection, sql)
  
  # Convert colum names to camelCase:
  colnames(covariates) <- SqlRender::snakeCaseToCamelCase(colnames(covariates))
  # Construct covariate reference:
  
  covariateRef <- data.frame(
    covariateId = covariateSettings$analysisId,
    covariateName = c('Outpatient visit at index'),
    analysisId = covariateSettings$analysisId,
    conceptId = -1
  )
  
  analysisRef <- data.frame(
    analysisId = covariateSettings$analysisId,
    analysisName = "visit type covariate",
    domainId = "visit type covariate",
    startDay = 0,
    endDay = 0,
    isBinary = "T",
    missingMeansZero = "T"
  )
  
  metaData <- list(sql = sql, call = match.call())
  result <- Andromeda::andromeda(
    covariates = covariates,
    covariateRef = covariateRef,
    analysisRef = analysisRef
  )
  
  attr(result, "metaData") <- metaData
  class(result) <- "CovariateData"	
  return(result)
}


#' Extracts visit location covariate
#'
#' @details
#' Determines the visit type for the cohort index
#'
#' @param analysisId  The analysisId for the covariate  
#'
#' @return
#' An object of class covariateSettings specifying how to create the VisitLocation covariate 
#'
#' @export
createVisitLocationCovariateSettings <- function(
  analysisId = 764
){
  
  covariateSettings <- list(
    analysisId = analysisId
  )
  
  attr(covariateSettings, "fun") <- "getVisitLocationCovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}