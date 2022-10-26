extractData <- function(
  databaseDetails,
  modelDesignList,
  logSettings,
  saveDirectory,
  useBulkLoad = T,
  outputFolder
){
  
  targetId <- unique(unlist(lapply(modelDesignList, function(x) x$targetId)))

    covariates <- list(
      modelDesignList[[1]]$covariateSettings[[1]],
      createPMCACovariateSettings(useBulkLoad = useBulkLoad),
      createVisitLocationCovariateSettings(),
      PatientLevelPrediction::createCohortCovariateSettings(
        cohortName = 'Positive covid test', 
        settingId = 1, 
        cohortDatabaseSchema = databaseDetails$cohortDatabaseSchema, 
        cohortTable = databaseDetails$cohortTable, 
        cohortId = 9086, 
        startDay = 0, 
        endDay = 0, 
        analysisId = 457
        )
    )
    
    databaseDetails$targetId <- targetId
    
    databaseDetails$outcomeIds <- unique(unlist(lapply(modelDesignList, function(x) x$outcomeId)))
    
    plpData <- PatientLevelPrediction::getPlpData(
      databaseDetails = databaseDetails, 
      covariateSettings = covariates, 
      restrictPlpDataSettings = modelDesignList[[1]]$restrictPlpDataSettings
    )
    
    matrixData <- convertData(plpData)
    
    # get the names for the cohort ids
    pathToCsv <- system.file("Cohorts.csv", package = "PASC")
    idToName <- utils::read.csv(pathToCsv)
    
    
    # create a data.frame with all 5 outcome labels
    allpop <- NULL
    for(outcomeId in databaseDetails$outcomeIds){
      pop <- PatientLevelPrediction::createStudyPopulation(
        plpData = plpData, 
        outcomeId = outcomeId, 
        populationSettings = modelDesignList[[1]]$populationSettings
      )
      
      pop <- pop %>% 
        dplyr::select(.data$rowId, .data$outcomeCount) 
      
      outcomeName <- idToName$cohortName[idToName$cohortId == outcomeId]
      colnames(pop)[2] <- outcomeName
      
      if(is.null(allpop)){
        allpop <- pop
      } else{
        allpop <- merge(allpop, pop, by='rowId', all = T)
      }
      
    }
    
    allData <- merge(matrixData, allpop, by = 'rowId', all.x=T)
    
    write.csv(
      x = allData, 
      file = file.path(outputFolder, paste0('data.csv')), 
      row.names = F
    )
    
return(invisible(allData))
}

convertData <- function(plpData){
  
  mappedData <- PatientLevelPrediction::toSparseM(
    plpData = plpData, 
    cohort = plpData$cohorts 
    )
  
#map Age categories: 0 to 11, 12 to 20 - covariate 1001? 
#map Time period of cohort entrance: 03/01/2020 to 02/28/2021 and 03/01/2021 to 12/31/2021
  
#Gender (male = 1) - covariate 850700x?
#Race: white vs non-white -  covariate 852700x?
#PMCA (Chromic conditions): 0, 1, 2 - covariates 0763,1763,2763,3763
#Test Location: Outpatient vs others - covariate 764
  
  matData <- mappedData$dataMatrix
  colnames(matData) <-  mappedData$covariateRef$covariateName
  matData <- as.data.frame(as.matrix(matData))
  # rename
  matData <- dplyr::rename(
    .data = matData, 
    gender = .data$`gender = MALE`,
    race = .data$`race = White`,
    PMCA_2 = .data$`PMCA complex chronic`,
    test_location = .data$`Outpatient visit at index`,
    PCA_positivity = .data$`Cohort_covariate during day 0 through 0 days relative to index:  Positive covid test  `
  ) %>%
    dplyr::mutate(
      PMCA_1 = (.data$`PMCA non-complex chronic` | .data$`PMCA non-complex and non-chonic`)*1,
    )
  
  colNamesOfInt <- c(
    "race",
    "gender", 
    "PMCA_1", 
    "PMCA_2", 
    "test_location",
    "PCA_positivity"
    )
  
  # convert matData$`age in years` 0 to 11, 12 to 20
  age <- (matData$`age in years` <= 11)*1
  #age12to20 <- (matData$`age in years` >= 12 & matData$`age in years` <= 20)*1
  # convert mappedData$labels[,c('rowId','cohortStartDate')] as 03/01/2020 to 02/28/2021 and 03/01/2021 to 12/31/2021
  entrace_time_1 <- (mappedData$labels$cohortStartDate >= as.Date('2020/03/01') & mappedData$labels$cohortStartDate <= as.Date('2021/02/28'))*1
  #mar21todec21 <- (mappedData$labels$cohortStartDate >= as.Date('2021/03/01') & mappedData$labels$cohortStartDate <= as.Date('2021/12/31'))*1
  
  allData <- cbind(
    mappedData$labels$rowId,
    matData[, colnames(matData)%in%colNamesOfInt], 
    age, 
    entrace_time_1
    )
  colnames(allData)[1] <- 'rowId'
  
  return(allData)
}