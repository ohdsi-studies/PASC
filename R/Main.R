# Copyright 2020 Observational Health Data Sciences and Informatics
#
# This file is part of PASC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Execute the Study
#'
#' @details
#' This function executes the PASC Study.
#' 
#' @param databaseDetails      Database details created using \code{PatientLevelPrediction::createDatabaseDetails()} 
#' @param outputFolder         Name of local folder to place results; make sure to use forward slashes
#'                             (/). Do not use a folder on a network drive since this greatly impacts
#'                             performance.
#' @param createCohorts        Create the cohortTable table with the target population and outcome cohorts?
#' @param getData              Extract the patient level data and save as a csv (this is not shared)
#' @param packageResults       Should results be packaged for later sharing?     
#' @param minCellCount         The minimum number of subjects contributing to a count before it can be included 
#'                             in packaged results.
#' @param sampleSize           The number of patients in the target cohort to sample (if NULL uses all patients)
#' @param logSettings           The log setting \code{PatientLevelPrediction::createLogSettings()}    
#' @param useBulkLoad           Use bulk load when inserting a large table into the database
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "postgresql",
#'                                              user = "joe",
#'                                              password = "secret",
#'                                              server = "myserver")
#'                                              
#'  databaseDetails <- PatientLevelPrediction::createDatabaseDetails(
#'  connectionDetails = connectionDetails,
#'  cdmDatabaseSchema = cdmDatabaseSchema,
#'  cdmDatabaseName = cdmDatabaseName,
#'  tempEmulationSchema = tempEmulationSchema,
#'  cohortDatabaseSchema = cohortDatabaseSchema,
#'  cohortTable = cohortTable,
#'  outcomeDatabaseSchema = cohortDatabaseSchema,
#'  outcomeTable = cohortTable,
#'  cdmVersion = cdmVersion
#'  )  
#'  
#'  logSettings <- PatientLevelPrediction::createLogSettings(
#'  verbosity = "INFO",
#'  timeStamp = T,
#'  logName = 'skeletonPlp'
#'  )                                          
#'
#' execute(databaseDetails = databaseDetails,
#'         outputFolder = "c:/temp/study_results", 
#'         createCohorts = T,
#'         getData = T,
#'         packageResults = F,
#'         minCellCount = 5,
#'         sampleSize = 10000,
#'         logSettings = logSettings
#'         )
#' }
#'
#' @export
execute <- function(
  databaseDetails,
  outputFolder,
  createCohorts = F,
  getData = F,
  packageResults = F,
  minCellCount= 5,
  sampleSize = NULL,
  logSettings,
  useBulkLoad = T
) {
  
  if (!file.exists(outputFolder))
    dir.create(outputFolder, recursive = TRUE)
  
  ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
  
  
  if (createCohorts) {
    ParallelLogger::logInfo("Creating cohorts")
    createCohorts(
      databaseDetails = databaseDetails,
      outputFolder = outputFolder
    )
  }
  
  if(getData){
    
    predictionAnalysisListFile <- system.file("settings",
      "predictionAnalysisList.json",
      package = "PASC")
    
    predictionAnalysisList <- tryCatch(
      {PatientLevelPrediction::loadPlpAnalysesJson(file.path(predictionAnalysisListFile))},
      error= function(cond) {
        ParallelLogger::logInfo('Issue when loading json file...');
        ParallelLogger::logError(cond);
        return(NULL)
      })
    
    
    # add sample settings
    if(!is.null(sampleSize)){
      ParallelLogger::logInfo('Adding sample settings')
      for(i in 1:length(predictionAnalysisList$analyses)){
        predictionAnalysisList$analyses[[i]]$restrictPlpDataSettings$sampleSize <- sampleSize
      }
    }
    
    # add code to add database settings for covariates...
    for(i in 1:length(predictionAnalysisList$analyses)){
      ParallelLogger::logInfo('Updating as cohort covariate settings is being used')
      predictionAnalysisList$analyses[[i]]$covariateSettings <- addCohortSettings(
        covariateSettings = predictionAnalysisList$analyses[[i]]$covariateSettings, 
        cohortDatabaseSchema = databaseDetails$cohortDatabaseSchema, 
        cohortTable = databaseDetails$cohortTable
      )
    }
    
    extractData(
        databaseDetails = databaseDetails,
        modelDesignList = predictionAnalysisList$analyses,
        logSettings = logSettings,
        saveDirectory = outputFolder,
        useBulkLoad = useBulkLoad,
        outputFolder = outputFolder
      )
  }
  
  
  
  # add code to package 

  invisible(NULL)
}




getNames <- function(
  cohortDefinitions, 
  ids
){
  
  idNames <- lapply(cohortDefinitions, function(x) c(x$id, x$name))
  idNames <- do.call(rbind, idNames)
  colnames(idNames) <- c('id', 'name')
  idNames <- as.data.frame(idNames)
  
  nams <- c()
  for(id in ids){
    nams <- c(nams, idNames$name[idNames$id == id])
  }
  
  return(nams)
  
}

