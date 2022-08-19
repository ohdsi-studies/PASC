#' Extracts PMCA value
#'
#' @details
#' The user specifies a cohort and the PMCA algorithm is calculated for each patient in the cohort
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
#' The PMCA covariate will now be added to the data
#'
#' @export
getPMCACovariateData <- function(
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
  
  pmcaData <- system.file('pmca_snomed.csv', package = 'PASC')
  pmcaData <- read.csv(pmcaData)
  # first bulk insert into database
  DatabaseConnector::insertTable(
    data = pmcaData,
    connection = connection, 
    tableName = '#pmca', 
    dropTableIfExists = T, 
    createTable = T, 
    tempTable = T, 
    tempEmulationSchema = oracleTempSchema, 
    bulkLoad = covariateSettings$useBulkLoad
      )
  
  # Some SQL to construct the covariate:
  sql <- paste(
    "IF OBJECT_ID('tempdb..#pmca_summary', 'U') IS NOT NULL
  DROP TABLE #pmca_summary;",
    
    "select row_id, body_system, 
    max(malignancy) as malignancy, 
    max(progressive) as progressive, 
    max(year1) year1, max(year2) year2, max(year3) year3,
    max(year1) + max(year2) + max(year3) as total_visits
    into #pmca_summary
    from ",
    "(",
    "select a.@row_id_field AS row_id, p.body_system, ",
    "case when p.progressive = 'yes' then 1 else 0 end progressive, ",
    "case when p.body_system = 'malignancy' then 1 else 0 end malignancy, ",
    "case when datediff(day,condition_start_date,cohort_start_date) <= 365 then 1 else 0 end year1, ",
    "case when datediff(day,condition_start_date,cohort_start_date) > 365 and 
               datediff(day,condition_start_date,cohort_start_date) <= 365*2 then 1 else 0 end year2, ",
    "case when datediff(day,condition_start_date,cohort_start_date) > 365*2 and 
               datediff(day,condition_start_date,cohort_start_date) <= 365*3 then 1 else 0 end year3 ",
    "from @cohort_temp_table a inner join @cdm_database_schema.condition_occurrence b",
    " on a.subject_id = b.person_id and ",
    " b.condition_start_date <= dateadd(day, @endDay, a.cohort_start_date) and ",
    " b.condition_start_date >= dateadd(day, @startDay, a.cohort_start_date) ",
    "inner join #pmca p on p.standard_code = b.condition_concept_id",
    " where p.DOMAIN_ID = 'Condition'",
    
    " union ",
    
    "select a.@row_id_field AS row_id, p.body_system, ",
    "case when p.progressive = 'yes' then 1 else 0 end progressive, ",
    "case when p.body_system = 'malignancy' then 1 else 0 end malignancy, ",
    "case when datediff(day,observation_date,cohort_start_date) <= 365 then 1 else 0 end year1, ",
    "case when datediff(day,observation_date,cohort_start_date) > 365 and 
               datediff(day,observation_date,cohort_start_date) <= 365*2 then 1 else 0 end year2, ",
    "case when datediff(day,observation_date,cohort_start_date) > 365*2 and 
               datediff(day,observation_date,cohort_start_date) <= 365*3 then 1 else 0 end year3 ",
    
    "from @cohort_temp_table a inner join @cdm_database_schema.observation b",
    " on a.subject_id = b.person_id and ",
    " b.observation_date <= dateadd(day, @endDay, a.cohort_start_date) and ",
    " b.observation_date >= dateadd(day, @startDay, a.cohort_start_date) ",
    "inner join #pmca p on p.standard_code = b.observation_concept_id",
    " where p.DOMAIN_ID = 'Observation'",
    ") temp_inner",
    " group  by row_id, body_system;"
  )
  
  sql <- SqlRender::render(
    sql,
    cohort_temp_table = cohortTable,
    row_id_field = rowIdField,
    startDay = covariateSettings$startDay,
    endDay = covariateSettings$endDay,
    cdm_database_schema = cdmDatabaseSchema
  )
  
  sql <- SqlRender::translate(
    sql = sql, 
    targetDialect = attr(connection, "dbms"),
    tempEmulationSchema = oracleTempSchema
  )
  
  DatabaseConnector::executeSql(connection, sql)
  
  sql <- paste("select a.row_id, ",
  "case when complex_chronic.row_id is not NULL then 3*1000+ @analysis_id ",
  " when complex_chronic.row_id is NULL and chronic_all.row_id is not NULL then 2*1000+ @analysis_id  ",
  " when complex_chronic.row_id is NULL and chronic_all.row_id is NULL and any_var.row_id is not NULL then 1*1000+ @analysis_id ",
  " else @analysis_id end covariate_id,",
  " 1 as covariate_value",
  "from @cohort_temp_table a left outer join ",
  "( select row_id from ",
  "(select row_id from #pmca_summary where (malignancy = 1 OR progressive = 1)",
  "union",
  "select row_id from (select row_id, count(distinct body_system) loc from #pmca_summary where total_visits >= 2 and malignancy = 0 group by row_id) temp",
  " where loc > 1) 
    temp_complex group by row_id) complex_chronic",
  " on a.row_id = complex_chronic.row_id",
  "left outer join ",
  "(select distinct row_id from #pmca_summary where total_visits >= 2 and malignancy = 0) chronic_all",
  " on a.row_id = chronic_all.row_id",
  "left outer join ",
  "(select distinct row_id from #pmca_summary) any_var",
  " on a.row_id = any_var.row_id;"
  )
  
  sql <- SqlRender::render(
    sql,
    cohort_temp_table = cohortTable,
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
    covariateId = (0:3)*1000 + covariateSettings$analysisId,
    covariateName = c('PMCA none','PMCA non-complex and non-chonic','PMCA non-complex chronic','PMCA complex chronic'),
    analysisId = rep(covariateSettings$analysisId, 4),
    conceptId = rep(-1,4)
  )
  
  analysisRef <- data.frame(
    analysisId = covariateSettings$analysisId,
    analysisName = "PMCA covariate",
    domainId = "PMCA covariate",
    startDay = covariateSettings$startDay,
    endDay = covariateSettings$endDay,
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


#' Extracts PMCA covariates
#'
#' @details
#' The user specifies a cohort and time period and then a covariate is constructed whether they are in the
#' cohort during the time periods relative to target population cohort index
#'
#' @param startDay  The number of days prior to index to start observing the cohort
#' @param endDay  The number of days prior to index to stop observing the cohort
#' @param analysisId  The analysisId for the covariate  
#' @param useBulkLoad Whether to use bulk upload for the PMCA variables        
#'
#' @return
#' An object of class covariateSettings specifying how to create the cohort covariate with the covariateId
#'  cohortId x 100000 + settingId x 1000 + analysisId
#'
#' @export
createPMCACovariateSettings <- function(
  startDay = -1096, 
  endDay = 0, 
  analysisId = 763,
  useBulkLoad = T
){
  
  covariateSettings <- list(
    startDay = startDay,
    endDay = endDay,
    analysisId = analysisId,
    useBulkLoad = useBulkLoad
  )
  
  attr(covariateSettings, "fun") <- "getPMCACovariateData"
  class(covariateSettings) <- "covariateSettings"
  return(covariateSettings)
}