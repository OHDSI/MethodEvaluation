# @file SignalInjection.R
#
# Copyright 2018 Observational Health Data Sciences and Informatics
#
# This file is part of MethodEvaluation
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

#' Inject signals in database
#'
#' @details
#' This function will insert additional outcomes for a given set of drug-outcome pairs. It is assumed
#' that these drug-outcome pairs represent negative controls, so the true relative risk before
#' inserting any outcomes should be 1. There are two models for inserting the outcomes during the
#' specified risk window of the drug: a Poisson model assuming multiple outcomes could occurr during a
#' single exposure, and a survival model considering only one outcome per exposure.
#' For each
#'
#' @param connectionDetails                An R object of type \code{ConnectionDetails} created using
#'                                         the function \code{createConnectionDetails} in the
#'                                         \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema                Name of database schema that contains OMOP CDM and
#'                                         vocabulary.
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you
#'                                         want all temporary tables to be managed. Requires
#'                                         create/insert permissions to this database.
#' @param exposureDatabaseSchema           The name of the database schema that is the location where
#'                                         the exposure data used to define the exposure cohorts is
#'                                         available.  If exposureTable = DRUG_ERA,
#'                                         exposureDatabaseSchema is not used by assumed to be
#'                                         cdmSchema.  Requires read permissions to this database.
#' @param exposureTable                    The table name that contains the exposure cohorts.  If
#'                                         exposureTable <> DRUG_ERA, then expectation is exposureTable
#'                                         has format of COHORT table: cohort_concept_id,
#'                                         SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema            The name of the database schema that is the location where
#'                                         the data used to define the outcome cohorts is available.
#'                                         If exposureTable = CONDITION_ERA, exposureDatabaseSchema is
#'                                         not used by assumed to be cdmSchema.  Requires read
#'                                         permissions to this database.
#' @param outcomeTable                     The table name that contains the outcome cohorts. When the table name is
#'                                         not CONDITION_ERA This table is expected
#'                                         to have the same format as the COHORT table:
#'                                         SUBJECT_ID, COHORT_START_DATE,
#'                                         COHORT_END_DATE, COHORT_CONCEPT_ID (CDM v4) or COHORT_DEFINITION_ID (CDM v5 and higher).
#' @param createOutputTable               Should the output table be created prior to inserting the
#'                                         outcomes? If TRUE and the tables already exists, it will
#'                                         first be deleted. If FALSE, the table is assumed to exist and the
#'                                         outcomes will be inserted. Any existing outcomes with the same IDs
#'                                         will first be deleted.
#' @param outputIdOffset            What should be the first new outcome ID that is to be created?
#' @param exposureOutcomePairs             A data frame with at least two columns:
#'                                         \itemize{
#'                                           \item {"exposureId" containing the drug_concept_ID
#'                                                 or cohort_concept_id of the exposure variable}
#'                                           \item {"outcomeId" containing the
#'                                                 condition_concept_ID or cohort_concept_id of the
#'                                                 outcome variable}
#'                                         }
#'
#' @param modelType                        Can be either "poisson" or "survival"
#' @param minOutcomeCountForModel          Minimum number of outcome events required to build a model.
#' @param minOutcomeCountForInjection      Minimum number of outcome events required to inject a signal.
#' @param covariateSettings                An object of type \code{covariateSettings} as created using the
#'                                         \code{createCovariateSettings} function in the
#'                                         \code{FeatureExtraction} package.
#' @param prior                 The prior used to fit the outcome model. See \code{\link[Cyclops]{createPrior}}
#'                              for details.
#' @param control               The control object used to control the cross-validation used to
#'                              determine the hyperparameters of the prior (if applicable). See
#'                              \code{\link[Cyclops]{createControl}} for details.
#' @param precision                        The allowed ratio between target and injected signal size.
#' @param firstExposureOnly                Should signals be injected only for the first exposure? (ie.
#'                                         assuming an acute effect)
#' @param washoutPeriod                    Number of days at the start of observation for which no
#'                                         signals will be injected, but will be used to determine
#'                                         whether exposure or outcome is the first one, and for
#'                                         extracting covariates to build the outcome model.
#' @param riskWindowStart                  The start of the risk window relative to the start of the
#'                                         exposure (in days). When 0, risk is assumed to start on the
#'                                         first day of exposure.
#' @param riskWindowEnd                    The end of the risk window relative to the start of the
#'                                         exposure. Note that typically the length of exposure is
#'                                         added to this number (when the \code{addExposureDaysToEnd}
#'                                         parameter is set to TRUE).
#' @param addExposureDaysToEnd             Should length of exposure be added to the risk window?
#' @param firstOutcomeOnly                 Should only the first outcome per person be considered when
#'                                         modeling the outcome?
#' @param removePeopleWithPriorOutcomes    Remove people with prior outcomes?
#' @param maxSubjectsForModel              Maximum number of people used to fit an outcome model.
#' @param effectSizes                      A numeric vector of effect sizes that should be inserted.
#' @param outputDatabaseSchema             The name of the database schema that is the location of the
#'                                         tables containing the new outcomesRequires write permissions
#'                                         to this database.
#' @param outputTable                      The name of the table names that will contain the generated outcome
#'                                         cohorts.
#' @param workFolder                       Path to a folder where intermediate data will be stored.
#' @param cdmVersion                       Define the OMOP CDM version used: currently support "4" and "5".
#' @param modelThreads                     Number of parallel threads to use when fitting outcome models.
#' @param generationThreads                Number of parallel threads to use when generating outcomes.
#'
#' @return
#' A data.frame listing all the drug-pairs in combination with requested effect sizes and the real inserted effect size
#' (might be different from the requested effect size because of sampling error).
#'
#' @export
injectSignals <- function(connectionDetails,
                          cdmDatabaseSchema,
                          oracleTempSchema = cdmDatabaseSchema,
                          exposureDatabaseSchema = cdmDatabaseSchema,
                          exposureTable = "drug_era",
                          outcomeDatabaseSchema = cdmDatabaseSchema,
                          outcomeTable = "cohort",
                          outputDatabaseSchema = outcomeDatabaseSchema,
                          outputTable = outcomeTable,
                          createOutputTable = FALSE,
                          exposureOutcomePairs,
                          modelType = "poisson",
                          minOutcomeCountForModel = 100,
                          minOutcomeCountForInjection = 25,
                          covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                                         useDemographicsGender = TRUE,
                                                                                         useDemographicsIndexYear = TRUE,
                                                                                         useDemographicsIndexMonth = TRUE,
                                                                                         useConditionGroupEraLongTerm = TRUE,
                                                                                         useDrugGroupEraLongTerm = TRUE,
                                                                                         useProcedureOccurrenceLongTerm = TRUE,
                                                                                         useMeasurementLongTerm = TRUE,
                                                                                         useObservationLongTerm = TRUE,
                                                                                         useCharlsonIndex = TRUE,
                                                                                         useDcsi = TRUE,
                                                                                         useChads2Vasc = TRUE,
                                                                                         longTermStartDays = 365,
                                                                                         endDays = 0),
                          prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                          control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10),
                          firstExposureOnly = FALSE,
                          washoutPeriod = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          firstOutcomeOnly = FALSE,
                          removePeopleWithPriorOutcomes = FALSE,
                          maxSubjectsForModel = 100000,
                          effectSizes = c(1, 1.25, 1.5, 2, 4),
                          precision = 0.01,
                          outputIdOffset = 1000,
                          workFolder = "./SignalInjectionTemp",
                          cdmVersion = "5",
                          modelThreads = 1,
                          generationThreads = 1) {
  if (min(effectSizes) < 1)
    stop("Effect sizes smaller than 1 are currently not supported")
  if (modelType != "poisson" && modelType != "survival")
    stop(paste0("Unknown modelType '", modelType, "', please select either 'poisson' or 'survival'"))
  if (cdmVersion == "4"){
    stop("CDM version 4 is not supported")
  } 
  if (!file.exists(workFolder)) {
    dir.create(workFolder)
  }
  exposuresFile <- file.path(workFolder, "exposures.rds")
  outcomesFile <- file.path(workFolder, "outcomes.rds")
  priorOutcomesFile <- file.path(workFolder, "priorOutcomes.rds")
  countsFile <- file.path(workFolder, "counts.rds")
  result <- data.frame(exposureId = rep(exposureOutcomePairs$exposureId,
                                        each = length(effectSizes)),
                       outcomeId = rep(exposureOutcomePairs$outcomeId,
                                       each = length(effectSizes)),
                       targetEffectSize = rep(effectSizes, nrow(exposureOutcomePairs)),
                       newOutcomeId = outputIdOffset+(0:(nrow(exposureOutcomePairs)*length(effectSizes)-1)),
                       trueEffectSize = 0,
                       trueEffectSizeFirstExposure = 0,
                       injectedOutcomes = 0,
                       modelFolder = "",
                       outcomesToInjectFile = "",
                       stringsAsFactors = FALSE)
  
  exposureIds <- unique(exposureOutcomePairs$exposureId)
  
  conn <- DatabaseConnector::connect(connectionDetails)
  
  # Create exposure cohorts ------------------------------------
  # cohortPersonCreated <- FALSE
  writeLines("\nCreating risk windows")
  renderedSql <- SqlRender::loadRenderTranslateSql("CreateExposedCohorts.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema,
                                                   cdm_database_schema = cdmDatabaseSchema,
                                                   exposure_ids = exposureIds,
                                                   washout_period = washoutPeriod,
                                                   exposure_database_schema = exposureDatabaseSchema,
                                                   exposure_table = exposureTable,
                                                   first_exposure_only = firstExposureOnly,
                                                   risk_window_start = riskWindowStart,
                                                   risk_window_end = riskWindowEnd,
                                                   add_exposure_days_to_end = addExposureDaysToEnd)
  
  DatabaseConnector::executeSql(conn, renderedSql)
  # cohortPersonCreated <- TRUE
  
  # Get exposure cohorts from database -------------------------------
  if (file.exists(exposuresFile)) {
    exposures <- readRDS(exposuresFile)
  } else {
    exposureSql <- SqlRender::loadRenderTranslateSql("GetExposedCohorts.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     oracleTempSchema = oracleTempSchema)
    exposures <- DatabaseConnector::querySql(conn, exposureSql)
    names(exposures) <- SqlRender::snakeCaseToCamelCase(names(exposures))
    exposures <- exposures[order(exposures$rowId), ]
    saveRDS(exposures, exposuresFile)
  }
  
  # Get prior outcomes (if needed) ------------------------------------ 
  if (removePeopleWithPriorOutcomes) {
    if (file.exists(priorOutcomesFile)) {
      priorOutcomes <- readRDS(priorOutcomesFile)
    } else {
      writeLines("Finding people with prior outcomes")
      table <- exposureOutcomePairs
      colnames(table) <- SqlRender::camelCaseToSnakeCase(colnames(table))
      DatabaseConnector::insertTable(connection = conn,
                                     tableName = "#exposure_outcome",
                                     data = table,
                                     dropTableIfExists = TRUE,
                                     createTable = TRUE,
                                     tempTable = TRUE,
                                     oracleTempSchema = oracleTempSchema)
      outcomeSql <- SqlRender::loadRenderTranslateSql("GetPriorOutcomes.sql",
                                                      packageName = "MethodEvaluation",
                                                      dbms = connectionDetails$dbms,
                                                      oracleTempSchema = oracleTempSchema,
                                                      cdm_database_schema = cdmDatabaseSchema,
                                                      outcome_database_schema = outcomeDatabaseSchema,
                                                      outcome_table = outcomeTable,
                                                      first_outcome_only = firstOutcomeOnly)
      priorOutcomes <- DatabaseConnector::querySql(conn, outcomeSql)
      names(priorOutcomes) <- SqlRender::snakeCaseToCamelCase(names(priorOutcomes))
      saveRDS(priorOutcomes, priorOutcomesFile)
      sql <- "TRUNCATE TABLE #exposure_outcome; DROP TABLE #exposure_outcome;"
      sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
      DatabaseConnector::executeSql(conn, sql)
    }
  }
  
  # Get outcomes ---------------------------------------
  if (file.exists(outcomesFile)) {
    outcomeCounts <- readRDS(outcomesFile)
  } else {
    writeLines("Extracting outcome counts")
    table <- exposureOutcomePairs
    colnames(table) <- SqlRender::camelCaseToSnakeCase(colnames(table))
    DatabaseConnector::insertTable(connection = conn,
                                   tableName = "#exposure_outcome",
                                   data = table,
                                   dropTableIfExists = TRUE,
                                   createTable = TRUE,
                                   tempTable = TRUE,
                                   oracleTempSchema = oracleTempSchema)
    outcomeSql <- SqlRender::loadRenderTranslateSql("GetOutcomes.sql",
                                                    packageName = "MethodEvaluation",
                                                    dbms = connectionDetails$dbms,
                                                    oracleTempSchema = oracleTempSchema,
                                                    cdm_database_schema = cdmDatabaseSchema,
                                                    outcome_database_schema = outcomeDatabaseSchema,
                                                    outcome_table = outcomeTable,
                                                    first_outcome_only = firstOutcomeOnly)
    outcomeCounts <- DatabaseConnector::querySql(conn, outcomeSql)
    names(outcomeCounts) <- SqlRender::snakeCaseToCamelCase(names(outcomeCounts))
    saveRDS(outcomeCounts, outcomesFile)
    sql <- "TRUNCATE TABLE #exposure_outcome; DROP TABLE #exposure_outcome;"
    sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
    DatabaseConnector::executeSql(conn, sql)
  }
  
  # Generate counts for result object -------------------------------
  if (file.exists(countsFile)) {
    result <- readRDS(countsFile)
  } else {
    writeLines("Computing counts per exposure - outcome pair")
    temp <- merge(exposures[, c("rowId", "exposureId", "eraNumber")], outcomeCounts[, c("rowId", "outcomeId", "y")])
    if (modelType == "survival") {
      temp$y <- temp$y != 0
    }
    observedOutcomes <- list()
    observedOutcomesFirstExposure <- list()
    exposureCounts <- list()
    firstExposureCounts <- list()
    for (outcomeId in unique(outcomeCounts$outcomeId)) {
      oTemp <- temp[temp$outcomeId == outcomeId, ]
      tempExposures <- exposures
      if (removePeopleWithPriorOutcomes) {
        removeRowIds <- priorOutcomes$rowId[priorOutcomes$outcomeId == outcomeId]
        oTemp <- oTemp[!(oTemp$rowId  %in% removeRowIds), ]
        tempExposures <- tempExposures[!(tempExposures$rowId  %in% removeRowIds), ]
      }
      tempAll <- aggregate(y ~ exposureId, oTemp, sum)
      tempAll$outcomeId <- outcomeId
      colnames(tempAll)[colnames(tempAll) == "y"] <- "observedOutcomes"
      observedOutcomes[[length(observedOutcomes) + 1]] <- tempAll
      
      tempFirst <- aggregate(y ~ exposureId, oTemp[oTemp$eraNumber == 1, ], sum)
      tempFirst$outcomeId <- outcomeId
      colnames(tempFirst)[colnames(tempFirst) == "y"] <- "observedOutcomesFirstExposure"
      observedOutcomesFirstExposure[[length(observedOutcomesFirstExposure) + 1]] <- tempFirst
      
      tempAll <- aggregate(rowId ~ exposureId, tempExposures, length)
      tempAll$outcomeId <- outcomeId
      colnames(tempAll)[colnames(tempAll) == "rowId"] <- "exposures"
      exposureCounts[[length(exposureCounts) + 1]] <- tempAll
      
      tempFirst <- aggregate(rowId ~ exposureId, tempExposures[tempExposures$eraNumber == 1, ], length)
      tempFirst$outcomeId <- outcomeId
      colnames(tempFirst)[colnames(tempFirst) == "rowId"] <- "firstExposures"
      firstExposureCounts[[length(firstExposureCounts) + 1]] <- tempFirst
    }
    result <- merge(result, do.call("rbind", observedOutcomes), all.x = TRUE)
    result$observedOutcomes[is.na(result$observedOutcomes)] <- 0
    result <- merge(result, do.call("rbind", observedOutcomesFirstExposure), all.x = TRUE)
    result$observedOutcomesFirstExposure[is.na(result$observedOutcomesFirstExposure)] <- 0
    result <- merge(result, do.call("rbind", exposureCounts), all.x = TRUE)
    result <- merge(result, do.call("rbind", firstExposureCounts), all.x = TRUE)
    saveRDS(result, countsFile)
  }
  
  # Build models ----------------------------
  # Find all exposures for each outcome, then identify unique groups of exposures
  group <- function(outcomeId) {
    exposureIds <- exposureOutcomePairs$exposureId[exposureOutcomePairs$outcomeId == outcomeId]
    return(exposureIds[order(exposureIds)])
  }
  outcomeIds <- unique(exposureOutcomePairs$outcomeId)
  groups <- sapply(unique(outcomeIds), group)
  uniqueGroups <- unique(groups)
  saveRDS(uniqueGroups, file.path(workFolder, "uniqueGroups.rds"))
  outcomeIdToGroupId <- data.frame(outcomeIds = outcomeIds, groupIds = match(groups, uniqueGroups))
  saveRDS(outcomeIdToGroupId, file.path(workFolder, "outcomeIdToGroupId.rds"))
  
  # Fetch covariates for each group of exposures
  for (i in 1:length(uniqueGroups)) {
    covarFileName <- file.path(workFolder, paste0("covarsForModel_g", i))
    if (!file.exists(covarFileName)) {
      cohortIds <- uniqueGroups[[i]]
      sql <- "SELECT COUNT(*) FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids);"
      sql <- SqlRender::renderSql(sql, cohort_ids = cohortIds)$sql
      sql <- SqlRender::translateSql(sql, 
                                     targetDialect = connectionDetails$dbms,
                                     oracleTempSchema = oracleTempSchema)$sql
      count <- DatabaseConnector::querySql(conn, sql)
      if (count > maxSubjectsForModel) {
        writeLines("Sampling exposed cohorts for model(s)")
        renderedSql <- SqlRender::loadRenderTranslateSql("SampleExposedCohorts.sql",
                                                         packageName = "MethodEvaluation",
                                                         dbms = connectionDetails$dbms,
                                                         oracleTempSchema = oracleTempSchema,
                                                         sample_size = maxSubjectsForModel,
                                                         cohort_ids = cohortIds)
        DatabaseConnector::executeSql(conn, renderedSql)
        
        sql <- "SELECT row_id FROM #sampled_person"
        sql <- SqlRender::translateSql(sql = sql, 
                                       targetDialect = connectionDetails$dbms,
                                       oracleTempSchema = oracleTempSchema)$sql
        sampledRowIds <- querySql(conn, sql)
        colnames(sampledRowIds) <- SqlRender::snakeCaseToCamelCase(colnames(sampledRowIds))
        sampledExposuresFile <- file.path(workFolder, paste0("sampledRowIds_g", i))
        saveRDS(sampledRowIds, sampledExposuresFile)
      } else {
        # Don't sample, just copy:
        sql <- "SELECT * INTO #sampled_person FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids);"
        sql <- SqlRender::renderSql(sql, cohort_ids = cohortIds)$sql
        sql <- SqlRender::translateSql(sql, 
                                       targetDialect = connectionDetails$dbms,
                                       oracleTempSchema = oracleTempSchema)$sql
        DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
      }
      writeLines("Extracting covariates for fitting outcome model(s)")
      covariateData <- FeatureExtraction::getDbCovariateData(connection = conn,
                                                             oracleTempSchema = oracleTempSchema,
                                                             cdmDatabaseSchema = cdmDatabaseSchema,
                                                             cohortTable = "#sampled_person",
                                                             cohortTableIsTemp = TRUE,
                                                             rowIdField = "row_id",
                                                             covariateSettings = covariateSettings,
                                                             cdmVersion = cdmVersion,
                                                             aggregated = FALSE)
      covariateData <- FeatureExtraction::tidyCovariateData(covariateData = covariateData,
                                                            normalize = TRUE,
                                                            removeRedundancy = TRUE)
      covariateRef <- covariateData$covariateRef
      covariates <- covariateData$covariates
      ffbase::save.ffdf(covariates, covariateRef, dir = covarFileName)
      
      sql <- "TRUNCATE TABLE #sampled_person; DROP TABLE #sampled_person;"
      sql <- SqlRender::translateSql(sql, 
                                     targetDialect = connectionDetails$dbms,
                                     oracleTempSchema = oracleTempSchema)$sql
      DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
  }
  
  writeLines("Fitting outcome models")
  tasks <- list()
  outcomeIds <- unique(exposureOutcomePairs$outcomeId)
  for (outcomeId in outcomeIds) {
    groupId <- outcomeIdToGroupId$groupId[outcomeIdToGroupId$outcomeId == outcomeId]
    groupExposureIds <- uniqueGroups[[groupId]]
    idx <- result$outcomeId == outcomeId & result$exposureId %in% groupExposureIds & result$targetEffectSize == effectSizes[1]
    if (sum(result$observedOutcomes[idx]) >= minOutcomeCountForModel) {
      modelFolder <- file.path(workFolder, paste0("model_o", outcomeId))
      result$modelFolder[result$outcomeId == outcomeId] <- modelFolder
      if (!file.exists(modelFolder)) {
        task <- list(outcomeId = outcomeId,
                     modelFolder = modelFolder,
                     covarFileName = file.path(workFolder, paste0("covarsForModel_g", groupId)),
                     sampledExposuresFile = file.path(workFolder, paste0("sampledRowIds_g", groupId)),
                     groupExposureIds = groupExposureIds)
        tasks[[length(tasks)+1]] <- task
      }
    }
  }
  
  if (length(tasks) > 0) {
    cluster <- OhdsiRTools::makeCluster(modelThreads)
    OhdsiRTools::clusterApply(cluster,
                              tasks,
                              fitModel,
                              result,
                              exposuresFile,
                              outcomesFile,
                              priorOutcomesFile,
                              removePeopleWithPriorOutcomes,
                              modelType,
                              prior,
                              control)
    OhdsiRTools::stopCluster(cluster)
  }
  
  # Generate new outcomes -----------------------------------------
  # Fetch covariates for all rows if covars for model were based on a sample:
  for (i in 1:length(uniqueGroups)) {
    sampledExposuresFile <- file.path(workFolder, paste0("sampledRowIds_g", i))
    covarFileName <- file.path(workFolder, paste0("covarsForPrediction_g", i))
    if (file.exists(sampledExposuresFile) && !file.exists(covarFileName)) {
      writeLines("Extracting covariates for all rows predicting outcomes")
      if (!is(covariateSettings, "covariateSettings")) {
        stop("Composite covariate settings not supported")
      }
      outcomeIds <- outcomeIdToGroupId$outcomeId[outcomeIdToGroupId$groupId == i]
      modelCovariateIds <- c()
      for (modelFolder in unique(result$modelFolder[result$outcomeId %in% outcomeIds])) {
        if (modelFolder != "" && file.exists(file.path(modelFolder, "betas.rds"))) {
          betas <- readRDS(file.path(modelFolder, "betas.rds"))
          modelCovariateIds <- c(modelCovariateIds, betas$id)
        }
      }
      modelCovariateIds <- unique(modelCovariateIds)
      modelCovariateIds <- modelCovariateIds[modelCovariateIds != 0]
      
      writeLines(paste("Number of unique covariates across outcome models:",length(modelCovariateIds)))
      if (length(modelCovariateIds) == 0) {
        dir.create(covarFileName)
      } else {
        covariateSettings$includedCovariateIds <- modelCovariateIds
        sql <- "SELECT * INTO #selected_person FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids);"
        sql <- SqlRender::renderSql(sql, cohort_ids = uniqueGroups[[i]])$sql
        sql <- SqlRender::translateSql(sql, 
                                       targetDialect = connectionDetails$dbms,
                                       oracleTempSchema = oracleTempSchema)$sql
        DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
        covariateData <- FeatureExtraction::getDbCovariateData(connection = conn,
                                                               oracleTempSchema = oracleTempSchema,
                                                               cdmDatabaseSchema = cdmDatabaseSchema,
                                                               cohortTable = "#selected_person",
                                                               cohortTableIsTemp = TRUE,
                                                               rowIdField = "row_id",
                                                               covariateSettings = covariateSettings,
                                                               cdmVersion = cdmVersion,
                                                               aggregated = FALSE)
        covariateData <- FeatureExtraction::tidyCovariateData(covariateData = covariateData,
                                                              normalize = TRUE,
                                                              removeRedundancy = FALSE)
        covariateRef <- covariateData$covariateRef
        covariates <- covariateData$covariates
        ffbase::save.ffdf(covariates, covariateRef, dir = covarFileName)
        
        sql <- "TRUNCATE TABLE #selected_person; DROP TABLE #selected_person;"
        sql <- SqlRender::translateSql(sql, 
                                       targetDialect = connectionDetails$dbms,
                                       oracleTempSchema = oracleTempSchema)$sql
        DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
      }
    }
  }
  
  writeLines("Generating outcomes")
  tasks <- list()
  for (exposureId in exposureIds) {
    outcomeIds <- unique(exposureOutcomePairs$outcomeId[exposureOutcomePairs$exposureId == exposureId])
    for (outcomeId in outcomeIds) {
      if (result$observedOutcomes[result$exposureId == exposureId & result$outcomeId == outcomeId][1] >= minOutcomeCountForInjection) {
        modelFolder <- result$modelFolder[result$exposureId == exposureId & result$outcomeId == outcomeId][1]
        if (modelFolder != "") {
          groupId <- outcomeIdToGroupId$groupId[outcomeIdToGroupId$outcomeId == outcomeId]
          sampledExposuresFile <- file.path(workFolder, paste0("sampledRowIds_g", groupId))
          if (file.exists(sampledExposuresFile)) {
            covarFileName <- file.path(workFolder, paste0("covarsForPrediction_g", groupId))
          } else {
            covarFileName <- file.path(workFolder, paste0("covarsForModel_g", groupId))
          }
          task <- list(exposureId = exposureId,
                       outcomeId = outcomeId,
                       modelFolder = modelFolder,
                       covarFileName = covarFileName)
          tasks[[length(tasks)+1]] <- task
        }
      }
    }
  }
  if (length(tasks) > 0) {
    cluster <- OhdsiRTools::makeCluster(generationThreads)
    results <- OhdsiRTools::clusterApply(cluster,
                                         tasks,
                                         generateOutcomes,
                                         result,
                                         exposuresFile,
                                         outcomesFile,
                                         priorOutcomesFile,
                                         removePeopleWithPriorOutcomes,
                                         modelType,
                                         effectSizes,
                                         precision,
                                         workFolder)
    OhdsiRTools::stopCluster(cluster)
    result <- do.call("rbind", results)
  }
  
  # Insert outcomes into database ------------------------------------------------
  writeLines("Inserting outcomes into database")
  outcomesToInject <- data.frame()
  for (i in 1:nrow(result)) {
    if (result$outcomesToInjectFile[i] != "") {
      outcomesToInject <- rbind(outcomesToInject, readRDS(result$outcomesToInjectFile[i]))
    }
  }
  colnames(outcomesToInject) <- SqlRender::camelCaseToSnakeCase(colnames(outcomesToInject))
  DatabaseConnector::insertTable(conn, "#temp_outcomes", outcomesToInject, TRUE, TRUE, TRUE, oracleTempSchema)
  
  toCopy <- result[result$modelFolder != "", c("outcomeId", "newOutcomeId")]
  colnames(toCopy) <- SqlRender::camelCaseToSnakeCase(colnames(toCopy))
  DatabaseConnector::insertTable(conn, "#to_copy", toCopy, TRUE, TRUE, TRUE, oracleTempSchema)
  
  copySql <- SqlRender::loadRenderTranslateSql("CopyOutcomes.sql",
                                               packageName = "MethodEvaluation",
                                               dbms = connectionDetails$dbms,
                                               cdm_database_schema = cdmDatabaseSchema,
                                               outcome_database_schema = outcomeDatabaseSchema,
                                               outcome_table = outcomeTable,
                                               output_database_schema = outputDatabaseSchema,
                                               output_table = outputTable,
                                               create_output_table = createOutputTable)
  
  DatabaseConnector::executeSql(conn, copySql)
  
  # if (cohortPersonCreated) {
  sql <- "TRUNCATE TABLE #cohort_person; DROP TABLE #cohort_person;"
  sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
  DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
  # }
  
  sql <- "TRUNCATE TABLE #temp_outcomes; DROP TABLE #temp_outcomes; TRUNCATE TABLE #to_copy; DROP TABLE #to_copy;"
  sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
  DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  DatabaseConnector::disconnect(conn)
  summaryFile <- .createSummaryFileName(workFolder)
  saveRDS(result, summaryFile)
  return(result)
}

fitModel <- function(task,
                     result,
                     exposuresFile,
                     outcomesFile,
                     priorOutcomesFile,
                     removePeopleWithPriorOutcomes,
                     modelType,
                     prior,
                     control) {
  exposures <- readRDS(exposuresFile)
  outcomes <- readRDS(outcomesFile)
  outcomes <- outcomes[outcomes$outcomeId == task$outcomeId, ]
  if (file.exists(task$sampledExposuresFile)) {
    sampledExposures <- readRDS(task$sampledExposuresFile)
    outcomes <- outcomes[outcomes$rowId %in% sampledExposures$rowId, ]
    exposures <- exposures[exposures$rowId %in% sampledExposures$rowId, ]
  } else {
    exposures <- exposures[exposures$exposureId %in% task$groupExposureIds, ]
    outcomes <- outcomes[outcomes$rowId %in% exposures$rowId, ]
  }
  
  ffbase::load.ffdf(task$covarFileName)
  open(covariates, readOnly = TRUE)
  open(covariateRef, readOnly = TRUE)
  covariates <- covariates[ffbase::'%in%'(covariates$rowId, exposures$rowId),]
  if (removePeopleWithPriorOutcomes) {
    priorOutcomes <- readRDS(priorOutcomesFile)
    removeRowIds <- priorOutcomes$rowId[priorOutcomes$outcomeId == task$outcomeId]
    outcomes <- outcomes[!(outcomes$rowId  %in% removeRowIds), ]
    exposures <- exposures[!(exposures$rowId  %in% removeRowIds), ]
    covariates <- covariates[!ffbase::'%in%'(covariates$rowId, removeRowIds),]
  }
  outcomes <- merge(exposures, outcomes[, c("rowId", "y", "timeToEvent")], by = c("rowId"), all.x = TRUE)
  outcomes <- outcomes[order(outcomes$rowId), ]
  outcomes$y[is.na(outcomes$y)] <- 0
  names(outcomes)[names(outcomes) == "daysAtRisk"] <- "time"
  if (modelType == "survival") {
    # For survival, time is either the time to the end of the risk window, or the event
    outcomes$y[outcomes$y != 0] <- 1
    outcomes$time[outcomes$y != 0] <- outcomes$timeToEvent[outcomes$y != 0]
  }
  outcomes$time <- outcomes$time + 1
  time <- outcomes$time
  firstExposureOutcomeCount <- sum(outcomes$y[outcomes$eraNumber == 1])
  
  # Note: for survival, using Poisson regression with 1 outcome and censored time as equivalent of
  # survival regression:
  cyclopsData <- Cyclops::convertToCyclopsData(ff::as.ffdf(outcomes), covariates, modelType = "pr", quiet = TRUE)
  fit <- tryCatch({
    Cyclops::fitCyclopsModel(cyclopsData, prior = prior, control = control)
  }, error = function(e) {
    e$message
  })
  if (fit$return_flag != "SUCCESS") {
    fit <- fit$return_flag
  }
  if (is.character(fit)) {
    writeLines(paste("Unable to fit model for outcome", task$outcomeId, ":", fit))
    dir.create(task$modelFolder)
    write.csv(fit, file.path(task$modelFolder, "Error.txt"))
  } else {
    betas <- coef(fit)
    intercept <- betas[1]
    betas <- betas[2:length(betas)]
    betas <- betas[betas != 0]
    if (length(betas) > 0){
      betas <- data.frame(beta = betas, id = as.numeric(attr(betas, "names")))
      betas <- merge(ff::as.ffdf(betas), covariateRef, by.x = "id", by.y = "covariateId")
      betas <- ff::as.ram(betas)
      betas <- betas[, c("beta", "id", "covariateName")]
      betas <- betas[order(-abs(betas$beta)), ]
    }
    betas <- rbind(data.frame(beta = intercept, id = 0, covariateName = "(Intercept)", row.names = NULL), betas)
    dir.create(task$modelFolder)
    saveRDS(betas, file.path(task$modelFolder, "betas.rds"))
  }
}

generateOutcomes <- function(task,
                             result,
                             exposuresFile,
                             outcomesFile,
                             priorOutcomesFile,
                             removePeopleWithPriorOutcomes,
                             modelType,
                             effectSizes,
                             precision,
                             workFolder) {
  resultSubset <- result[result$exposureId == task$exposureId & result$outcomeId == task$outcomeId, ]
  if (file.exists(file.path(task$modelFolder, "Error.txt"))) {
    return(resultSubset)
  } else {
    exposures <- readRDS(exposuresFile)
    exposures <- exposures[exposures$exposureId == task$exposureId, ]
    predictionFile <- file.path(task$modelFolder, paste0("prediction_e", task$exposureId,".rds"))    
    if (file.exists(predictionFile)) {
      prediction <- readRDS(predictionFile)
    } else {
      betas <- readRDS(file.path(task$modelFolder, "betas.rds"))
      if (nrow(betas) == 1) {
        covariates <- NULL 
      } else {
        ffbase::load.ffdf(task$covarFileName)
        open(covariates, readOnly = TRUE)
        covariates <- covariates[ffbase::`%in%`(covariates$rowId, exposures$rowId), ]
      }
      prediction <- .predict(betas, exposures, covariates, modelType)
      saveRDS(prediction, predictionFile)    
    }
    outcomes <- readRDS(outcomesFile)
    outcomes <- outcomes[outcomes$outcomeId == task$outcomeId, ]
    if (removePeopleWithPriorOutcomes) {
      priorOutcomes <- readRDS(priorOutcomesFile)
      removeRowIds <- priorOutcomes$rowId[priorOutcomes$outcomeId == task$outcomeId]
      exposures <- exposures[!(exposures$rowId  %in% removeRowIds), ]
      outcomes <- outcomes[!(outcomes$rowId  %in% removeRowIds), ]
    }
    exposures <- merge(exposures, prediction)
    exposures <- merge(exposures, outcomes, all.x = TRUE)
    exposures$hasOutcome <- !is.na(exposures$timeToEvent)
    exposures$daysAtRisk[exposures$hasOutcome] <- exposures$timeToEvent[exposures$hasOutcome]
    for (fxSizeIdx in 1:length(effectSizes)) {
      effectSize <- effectSizes[fxSizeIdx]
      if (effectSize == 1) {
        newOutcomes <- data.frame()
        injectedRr <- 1
        injectedRrFirstExposure <- 1
      } else {
        # When sampling, the expected RR size is the target RR, but the actual RR could be different due to
        # random error.  this code is redoing the sampling until actual RR is equal to the target RR.
        targetCount <- resultSubset$observedOutcomes[1] * (effectSize - 1)
        time <- exposures$daysAtRisk + 1
        newOutcomeCounts <- 0
        if (modelType == "poisson") {
          # Generate results under Poisson model -------------------------
          multiplier <- 1
          ratios <- c()
          while (round(abs(sum(newOutcomeCounts) - targetCount)) > precision * targetCount){
            newOutcomeCounts <- rpois(nrow(exposures), multiplier * exposures$prediction * (effectSize - 1))
            newOutcomeCounts[newOutcomeCounts > time] <- time[newOutcomeCounts > time]
            ratios <- c(ratios, sum(newOutcomeCounts) / targetCount)
            if (length(ratios) == 100){
              multiplier <- multiplier * 1/mean(ratios)
              ratios <- c()
              writeLines(paste("Unable to achieve target RR using model as is. Adding multiplier of", multiplier, "to force target"))
            }
          }
          idx <- which(newOutcomeCounts != 0)
          temp <- data.frame(personId = exposures$personId[idx],
                             cohortStartDate = exposures$cohortStartDate[idx],
                             time = exposures$daysAtRisk[idx] + 1,
                             nOutcomes = newOutcomeCounts[idx])
          outcomeRows <- sum(temp$nOutcomes)
          newOutcomes <- data.frame(personId = rep(0, outcomeRows),
                                    cohortStartDate = rep(as.Date("1900-01-01"), outcomeRows),
                                    timeToEvent = rep(0, outcomeRows))
          cursor <- 1
          for (i in 1:nrow(temp)) {
            nOutcomes <- temp$nOutcomes[i]
            if (nOutcomes != 0) {
              newOutcomes$personId[cursor:(cursor + nOutcomes - 1)] <- temp$personId[i]
              newOutcomes$cohortStartDate[cursor:(cursor + nOutcomes - 1)] <- temp$cohortStartDate[i]
              newOutcomes$timeToEvent[cursor:(cursor + nOutcomes - 1)] <- sample.int(size = nOutcomes,
                                                                                     temp$time[i]) - 1
              cursor <- cursor + nOutcomes
            }
          }
          injectedRr <- 1 + (nrow(newOutcomes)/resultSubset$observedOutcomes[1])
          
          # Count outcomes during first episodes:
          newOutcomeCountsFirstExposure <- sum(newOutcomeCounts[exposures$eraNumber == 1])
          injectedRrFirstExposure <- 1 + (newOutcomeCountsFirstExposure/resultSubset$observedOutcomesFirstExposure[1])
        } else { # Survival model
          # Generate outcomes under survival model --------------------------------------
          correctedTargetCount <- targetCount
          ratios <- c()
          multiplier <- 1
          temp <- c()
          while (round(abs(sum(temp) - correctedTargetCount)) > precision * correctedTargetCount) {
            timeToEvent <- round(rexp(nrow(exposures), multiplier * exposures$prediction * (effectSize - 1)))
            temp <- timeToEvent < time
            # Correct for censored time and outcomes:
            correctedTargetCount <- (resultSubset$observedOutcomes[1] / sum(time)) * (sum(time[!temp]) + sum(timeToEvent[temp] + 1)) * effectSize - sum(exposures$hasOutcome[!temp])
            ratios <- c(ratios, sum(temp) / correctedTargetCount)
            if (length(ratios) == 100){
              multiplier <- multiplier * 1/mean(ratios)
              ratios <- c()
              writeLines(paste("Unable to achieve target RR using model as is. Adding multiplier of", multiplier, "to force target"))
            }
            if (is.na(round(abs(sum(temp) - correctedTargetCount)))) {
              writeLines("Problem")
            }
          }
          rateBefore <- resultSubset$observedOutcomes[1] / sum(time)
          rateAfter <- (sum(exposures$hasOutcome[!temp]) + sum(temp)) / (sum(time[!temp]) + sum(timeToEvent[temp] + 1))
          injectedRr <- rateAfter / rateBefore
          newOutcomes <- data.frame(personId = exposures$personId[temp],
                                    cohortStartDate = exposures$cohortStartDate[temp],
                                    timeToEvent = timeToEvent[temp])
          # Count outcomes during first episodes:
          if (max(exposures$eraNumber) == 1){
            injectedRrFirstExposure <- injectedRr
          } else {
            firstTemp <- temp[exposures$eraNumber == 1]
            firstTimeToEvent <- timeToEvent[exposures$eraNumber == 1]
            firstExposures <- exposures[exposures$eraNumber == 1, ]
            firstTime <- time[exposures$eraNumber == 1]
            rateBeforeFirstExposure <- resultSubset$observedOutcomesFirstExposure[1] / sum(firstTime)
            rateAfterFirstExposure <- (sum(firstExposures$hasOutcome[!firstTemp]) + sum(firstTemp)) / (sum(firstTime[!firstTemp]) + sum(firstTimeToEvent[firstTemp] + 1))
            injectedRrFirstExposure <- rateAfterFirstExposure / rateBeforeFirstExposure
          }
        }
      }
      writeLines(paste("Target RR =",
                       effectSize,
                       ", injected RR =",
                       injectedRr,
                       ", injected RR during first exposure only =",
                       injectedRrFirstExposure))
      
      newOutcomeId <- resultSubset$newOutcomeId[resultSubset$targetEffectSize == effectSize]
      # Write new outcomes to file for later insertion into DB:
      if (nrow(newOutcomes) != 0) {
        newOutcomes$cohortStartDate <- newOutcomes$cohortStartDate + newOutcomes$timeToEvent
        newOutcomes$timeToEvent <- NULL
        newOutcomes$cohortDefinitionId <- newOutcomeId
        names(newOutcomes)[names(newOutcomes) == "personId"] <- "subjectId"
        outcomesToInjectFile <- file.path(workFolder, paste0("newOutcomes_e", task$exposureId, "_o", task$outcomeId, "_rr", effectSize , ".rds"))
        saveRDS(newOutcomes, outcomesToInjectFile)
        resultSubset$outcomesToInjectFile[resultSubset$targetEffectSize == effectSize] <- outcomesToInjectFile
      }
      idx <- resultSubset$targetEffectSize == effectSize
      resultSubset$trueEffectSize[idx] <- injectedRr
      resultSubset$trueEffectSizeFirstExposure[idx] <- injectedRrFirstExposure
      resultSubset$injectedOutcomes[idx] <- nrow(newOutcomes)
    }
    return(resultSubset)
  }
}

plotCalibration <- function(prediction, y, time) {
  # time <- ff::as.ram(outcomes$time) y <- ff::as.ram(outcomes$y) This is quite complicated because
  # it's a Poisson model. Creating equal sized bins based on days.
  numberOfStrata <- 5
  data <- data.frame(predRate = prediction/time, obs = y, time = time)
  data <- data[order(data$predRate), ]
  data$cumTime <- cumsum(data$time)
  q <- quantile(data$cumTime, (1:(numberOfStrata - 1))/numberOfStrata)
  data$strata <- cut(data$cumTime, breaks = c(0, q, max(data$cumTime)), labels = FALSE)
  strataData <- merge(aggregate(obs ~ strata, data = data, sum),
                      aggregate(time ~ strata, data = data, sum))
  
  strataData$rate <- strataData$obs/strataData$time
  temp <- aggregate(predRate ~ strata, data = data, min)
  names(temp)[names(temp) == "predRate"] <- "minx"
  strataData <- merge(strataData, temp)
  strataData$maxx <- c(strataData$minx[2:numberOfStrata], max(data$predRate))
  
  # Do not show last percent of data (in days):
  limx <- min(data$predRate[data$cumTime > 0.99 * sum(data$time)])
  ggplot2::ggplot(strataData, ggplot2::aes(xmin = minx,
                                           xmax = maxx,
                                           ymin = 0,
                                           ymax = rate)) + ggplot2::geom_abline() + ggplot2::geom_rect(color = rgb(0, 0, 0.8, alpha = 0.8),
                                                                                                       fill = rgb(0,
                                                                                                                  0,
                                                                                                                  0.8,
                                                                                                                  alpha = 0.5)) + ggplot2::coord_cartesian(xlim = c(0, limx))
}

.createSummaryFileName <- function(folder) {
  name <- "summary"
  name <- paste(name, ".rds", sep = "")
  return(file.path(folder, name))
}

.predict <- function(betas, exposures, covariates, modelType) {
  intercept <- betas$beta[1]
  if (nrow(betas) == 1) {
    betas <- data.frame()
  } else {
    betas <- betas[2:length(betas), ]
    colnames(betas)[colnames(betas) == "id"] <- "covariateId"
  }
  if (nrow(betas) == 0) {
    prediction <- data.frame(rowId = exposures$rowId,
                             daysAtRisk = exposures$daysAtRisk,
                             value = exp(intercept))
  } else {
    prediction <- ffbase::merge.ffdf(covariates, ff::as.ffdf(betas[, c("covariateId", "beta")]))
    if (is.null(prediction)) {
      prediction <- data.frame(rowId = exposures$rowId,
                               daysAtRisk = exposures$daysAtRisk,
                               value = exp(intercept))
    } else {
      prediction$value <- prediction$covariateValue * prediction$beta
      prediction <- FeatureExtraction::bySumFf(prediction$value, prediction$rowId)
      colnames(prediction) <- c("rowId", "value")
      prediction <- merge(exposures[, c("rowId", "daysAtRisk")], prediction, by = "rowId", all.x = TRUE)
      prediction$value[is.na(prediction$value)] <- 0
      prediction$value <- exp(prediction$value + intercept)
    }
  }
  if (modelType == "poisson") {
    prediction$value <- prediction$value * (prediction$daysAtRisk + 1)
  }
  prediction$daysAtRisk <- NULL
  colnames(prediction)[colnames(prediction) == "value"] <- "prediction"
  return(prediction)
}

