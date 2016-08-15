# @file SignalInjection.R
#
# Copyright 2016 Observational Health Data Sciences and Informatics
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
#' @param buildOutcomeModel                Should an outcome model be created to predict outcomes. New
#'                                         outcomes will be inserted based on the predicted
#'                                         probabilities according to this model, and this will help
#'                                         preserve the observed confounding when injecting signals.
#' @param buildModelPerExposure            If TRUE, an outcome model will be created for each exposure ID. IF false,
#'                                         outcome models will be created across all exposures. 
#' @param minOutcomeCount                  Minimum number of outcome events required to build a model and inject a signal.
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
#' @param effectSizes                      A numeric vector of effect sizes that should be inserted.
#' @param outputDatabaseSchema             The name of the database schema that is the location of the
#'                                         tables containing the new outcomesRequires write permissions
#'                                         to this database.
#' @param outputTable                      The name of the table names that will contain the generated outcome
#'                                         cohorts.
#' @param workFolder                       Path to a folder where intermediate data will be stored.
#' @param cdmVersion                   Define the OMOP CDM version used: currently support "4" and "5".
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
                          buildOutcomeModel = TRUE,
                          buildModelPerExposure = FALSE,
                          minOutcomeCount = 100,
                          covariateSettings = FeatureExtraction::createCovariateSettings(useCovariateDemographics = TRUE,
                                                                                         useCovariateConditionOccurrence = TRUE,
                                                                                         useCovariateConditionOccurrence365d = TRUE,
                                                                                         useCovariateConditionOccurrence30d = TRUE,
                                                                                         useCovariateConditionOccurrenceInpt180d = TRUE,
                                                                                         useCovariateConditionEra = TRUE,
                                                                                         useCovariateConditionEraEver = TRUE,
                                                                                         useCovariateConditionEraOverlap = TRUE,
                                                                                         useCovariateConditionGroup = TRUE,
                                                                                         useCovariateDrugExposure = TRUE,
                                                                                         useCovariateDrugExposure365d = TRUE,
                                                                                         useCovariateDrugExposure30d = TRUE,
                                                                                         useCovariateDrugEra = TRUE,
                                                                                         useCovariateDrugEra365d = TRUE,
                                                                                         useCovariateDrugEra30d = TRUE,
                                                                                         useCovariateDrugEraEver = TRUE,
                                                                                         useCovariateDrugEraOverlap = TRUE,
                                                                                         useCovariateDrugGroup = TRUE,
                                                                                         useCovariateProcedureOccurrence = TRUE,
                                                                                         useCovariateProcedureOccurrence365d = TRUE,
                                                                                         useCovariateProcedureOccurrence30d = TRUE,
                                                                                         useCovariateProcedureGroup = TRUE,
                                                                                         useCovariateObservation = TRUE,
                                                                                         useCovariateObservation365d = TRUE,
                                                                                         useCovariateObservation30d = TRUE,
                                                                                         useCovariateObservationCount365d = TRUE,
                                                                                         useCovariateMeasurement365d = TRUE,
                                                                                         useCovariateMeasurement30d = TRUE,
                                                                                         useCovariateMeasurementCount365d = TRUE,
                                                                                         useCovariateMeasurementBelow = TRUE,
                                                                                         useCovariateMeasurementAbove = TRUE,
                                                                                         useCovariateConceptCounts = TRUE,
                                                                                         useCovariateRiskScores = TRUE,
                                                                                         useCovariateRiskScoresCharlson = TRUE,
                                                                                         useCovariateRiskScoresDCSI = TRUE,
                                                                                         useCovariateRiskScoresCHADS2 = TRUE,
                                                                                         useCovariateRiskScoresCHADS2VASc = TRUE,
                                                                                         useCovariateInteractionYear = FALSE,
                                                                                         useCovariateInteractionMonth = FALSE,
                                                                                         excludedCovariateConceptIds = c(),
                                                                                         deleteCovariatesSmallCount = 100),
                          prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                          control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10),
                          firstExposureOnly = FALSE,
                          washoutPeriod = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          firstOutcomeOnly = FALSE,
                          effectSizes = c(1, 1.25, 1.5, 2, 4),
                          precision = 0.01,
                          outputIdOffset = 1000,
                          workFolder = "./SignalInjectionTemp",
                          cdmVersion = "4",
                          outcomeThreads = 1) {
  if (min(effectSizes) < 1)
    stop("Effect sizes smaller than 1 are currently not supported")
  if (modelType != "poisson" && modelType != "survival")
    stop(paste0("Unknown modelType '", modelType, "', please select either 'poisson' or 'survival'"))
  if (cdmVersion == "4"){
    cohortDefinitionId <- "cohort_concept_id"
  } else {
    cohortDefinitionId <- "cohort_definition_id"
  }
  if (!file.exists(workFolder))
    dir.create(workFolder)
  
  exposureIds <- unique(exposureOutcomePairs$exposureId)
  conn <- DatabaseConnector::connect(connectionDetails)
  
  ### Create all exposure cohorts ###
  writeLines("\nCreating risk windows")
  renderedSql <- SqlRender::loadRenderTranslateSql("CreateExposedCohorts.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema,
                                                   cdm_database_schema = cdmDatabaseSchema,
                                                   exposure_ids = exposureId,
                                                   washout_period = washoutPeriod,
                                                   exposure_database_schema = exposureDatabaseSchema,
                                                   exposure_table = exposureTable,
                                                   first_exposure_only = firstExposureOnly,
                                                   risk_window_start = riskWindowStart,
                                                   risk_window_end = riskWindowEnd,
                                                   add_exposure_days_to_end = addExposureDaysToEnd,
                                                   cohort_definition_id = cohortDefinitionId)
  
  DatabaseConnector::executeSql(conn, renderedSql)
  exposuresFile <- file.path(workFolder, "exposures.rds")
  if (!file.exists(exposuresFile)) {
    exposureSql <- SqlRender::loadRenderTranslateSql("GetExposedCohorts.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     oracleTempSchema = oracleTempSchema,
                                                     cohort_definition_id = cohortDefinitionId)
    exposures <- DatabaseConnector::querySql(conn, exposureSql)
    names(exposures) <- SqlRender::snakeCaseToCamelCase(names(exposures))
    names(exposures)[names(exposures) == "subjectId"] <- "personId"
    saveRDS(exposures, exposuresFile)
  }
  
  writeLines("Extracting outcome counts")
  outcomesFile <- file.path(workFolder, "outcomes.rds")
  if (!file.exists(outcomesFile)) {
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
                                                    first_outcome_only = firstOutcomeOnly,
                                                    cohort_definition_id = cohortDefinitionId)
    outcomeCounts <- DatabaseConnector::querySql(conn, outcomeSql)
    names(outcomeCounts) <- SqlRender::snakeCaseToCamelCase(names(outcomeCounts))
    names(outcomeCounts)[names(outcomeCounts) == "subjectId"] <- "personId"
    sql <- "TRUNCATE TABLE #exposure_outcome; DROP TABLE #exposure_outcome;"
    sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
    DatabaseConnector::executeSql(conn, sql)
    saveRDS(outcomeCounts, outcomesFile)
  }
  
  if (buildOutcomeModel) {
    covarDataFolder <- file.path(workFolder, "covariates")
    if (!file.exists(covarDataFolder)) {
      writeLines("Extracting covariates for the outcome model")
      covariates <- FeatureExtraction::getDbCovariateData(connection = conn,
                                                          oracleTempSchema = oracleTempSchema,
                                                          cdmDatabaseSchema = cdmDatabaseSchema,
                                                          cohortTable = "#cohort_person",
                                                          cohortTableIsTemp = TRUE,
                                                          cohortIds = exposureId,
                                                          rowIdField = "row_id",
                                                          covariateSettings = covariateSettings,
                                                          cdmVersion = cdmVersion)
      covariateRef <- covariates$covariateRef
      covariates <- covariates$covariates
      ffbase::save.ffdf(covariates, covariateRef, dir = covarDataFolder)
    }
    if (buildModelPerExposure) {
      ## This is where I stopped
    } else {
      
    }
  }  

  for (exposureId in exposureIds) {
    # exposureId = exposureIds[1]
    writeLines(paste("\nProcessing exposure", exposureId))
    outcomeIds <- unique(exposureOutcomePairs$outcomeId[exposureOutcomePairs$exposureId == exposureId])
    
 
    
    writeLines("Fitting outcome models and generating outcomes")
    tasks <- list()
    for (i in 1:length(outcomeIds)) {
      task <- list(outcomeId = outcomeIds[i],
                   newOutcomeIdOffet = outputIdOffset + ((i-1)*length(effectSizes)))
      tasks[[length(tasks)+1]] <- task
    }
    cluster <- OhdsiRTools::makeCluster(outcomeThreads)
    results <- OhdsiRTools::clusterApply(cluster,
                                         tasks,
                                         fitModelAndGenerateOutcomes,
                                         exposureId,
                                         effectSizes,
                                         minOutcomeCount,
                                         modelType,
                                         precision,
                                         prior,
                                         control,
                                         workFolder)
    OhdsiRTools::stopCluster(cluster)
    result <- do.call("rbind", results)
  }
}

writeLines("Inserting outcomes into database")
outcomesToInject <- data.frame()
for (i in 1:nrow(result)) {
  if (result$outcomesToInjectFile[i] != "") {
    outcomesToInject <- rbind(outcomesToInject, readRDS(result$outcomesToInjectFile[i]))
  }
}
if (cdmVersion == "4") {
  colnames(outcomesToInject)[colnames(outcomesToInject) == "cohortDefinitionId"] <- "cohortConceptId"
}
colnames(outcomesToInject) <- SqlRender::camelCaseToSnakeCase(colnames(outcomesToInject))
DatabaseConnector::insertTable(conn, "#temp_outcomes", outcomesToInject, TRUE, TRUE, TRUE, oracleTempSchema)

if (createOutputTable){
  sql <- "IF OBJECT_ID('@output_database_schema.@output_table', 'U') IS NOT NULL\nDROP TABLE @output_database_schema.@output_table;\n"
  sql <- paste(sql, "SELECT @cohort_definition_id, cohort_start_date, cohort_end_date, subject_id INTO @output_database_schema.@output_table FROM (\n")
} else{
  sql <- "DELETE FROM @output_database_schema.@output_table WHERE @cohort_definition_id IN (@new_outcome_ids);\n"
  sql <- paste(sql, "INSERT INTO @output_database_schema.@output_table (@cohort_definition_id, cohort_start_date, cohort_end_date, subject_id)\n")
}
sql <- paste(sql, "SELECT @cohort_definition_id, cohort_start_date, NULL AS cohort_end_date, subject_id FROM #temp_outcomes UNION ALL\n")
sql <- SqlRender::renderSql(sql,
                            output_database_schema = outputDatabaseSchema,
                            output_table = outputTable,
                            new_outcome_ids = result$newOutcomeId,
                            cohort_definition_id = cohortDefinitionId)$sql
sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
# Copy outcomes to output table:
outcomesCopySql <- c()
for (i in 1:nrow(result)) {
  if (result$modelFile[i] != "") {
    copySql <- SqlRender::loadRenderTranslateSql("CopyOutcomes.sql",
                                                 packageName = "MethodEvaluation",
                                                 dbms = connectionDetails$dbms,
                                                 cdm_database_schema = cdmDatabaseSchema,
                                                 outcome_database_schema = outcomeDatabaseSchema,
                                                 outcome_table = outcomeTable,
                                                 source_concept_id = result$outcomeId[i],
                                                 target_concept_id =  result$newOutcomeId[i],
                                                 cohort_definition_id = cohortDefinitionId)
    outcomesCopySql <- c(outcomesCopySql, copySql)
  }
}
sql <- paste(sql, paste(outcomesCopySql, collapse = " UNION ALL "))
if (createOutputTable){
  sql <- paste(sql, ") temp;")
} else {
  sql <- paste(sql, ";")
}

DatabaseConnector::executeSql(conn, sql)

sql <- "TRUNCATE TABLE #cohort_person; DROP TABLE #cohort_person; TRUNCATE TABLE #temp_outcomes; DROP TABLE #temp_outcomes;"
sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)

RJDBC::dbDisconnect(conn)
summaryFile <- .createSummaryFileName(workFolder)
saveRDS(result, summaryFile)
return(result)
}

fitModelAndGenerateOutcomes <- function(task,
                                        exposureId,
                                        effectSizes,
                                        minOutcomeCount,
                                        modelType,
                                        precision,
                                        prior,
                                        control,
                                        workFolder) {
  outcomeId <- task$outcomeId
  newOutcomeIdOffet <- task$newOutcomeIdOffet
  covarDataFolder <- file.path(workFolder, paste0("covariates_e", exposureId))
  ffbase::load.ffdf(covarDataFolder)
  open(covariates, readOnly = TRUE)
  open(covariateRef, readOnly = TRUE)
  
  outcomesFile <- file.path(workFolder, paste0("outcomes_e", exposureId, ".rds"))
  outcomeCounts <- readRDS(outcomesFile)
  
  exposuresFile <- file.path(workFolder, paste0("exposures_e", exposureId, ".rds"))
  exposures <- readRDS(exposuresFile)
  
  result <- data.frame(exposureId = exposureId,
                       outcomeId = outcomeId,
                       targetEffectSize = effectSizes,
                       newOutcomeId = newOutcomeIdOffet+(1:length(effectSizes))-1,
                       trueEffectSize = 0,
                       trueEffectSizeFirstExposure = 0,
                       exposures = nrow(exposures),
                       firstExposures = sum(exposures$eraNumber == 1),
                       observedOutcomes = 0,
                       injectedOutcomes = 0,
                       modelFile = "",
                       outcomesToInjectFile = "",
                       stringsAsFactors = FALSE)
  
  if (sum(outcomeCounts$outcomeConceptId == outcomeId) < minOutcomeCount){
    writeLines(paste("Too few outcomes found for outcome", outcomeId))
    result$observedOutcomes <- sum(outcomeCounts$outcomeConceptId == outcomeId)
  } else {
    outcomes <- outcomeCounts[outcomeCounts$outcomeConceptId == outcomeId, ]
    outcomes <- merge(exposures, outcomes, by = c("cohortStartDate", "personId"), all.x = TRUE)
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
    modelFolder <- file.path(workFolder, paste0("model_e", exposureId, "_o", outcomeId))
    haveModel <- FALSE
    if (file.exists(modelFolder)){
      if (!file.exists(file.path(modelFolder, "Error.txt"))) {
        prediction <- readRDS(file.path(modelFolder, "prediction.rds"))
        betas <- readRDS(file.path(modelFolder, "betas.rds"))
        haveModel <- TRUE
      }
    } else {
      # Note: for survival, using Poisson regression with 1 outcome and censored time as equivalent of
      # survival regression:
      cyclopsData <- Cyclops::convertToCyclopsData(ff::as.ffdf(outcomes), covariates, modelType = "pr", quiet = TRUE)
      fit <- tryCatch({
        Cyclops::fitCyclopsModel(cyclopsData, prior = prior, control = control)
      }, error = function(e) {
        e$message
      })
      if (fit$return_flag != "SUCCESS")
        fit <- fit$return_flag
      if (is.character(fit)) {
        writeLines(paste("Unable to fit model for exposure", exposureId, "and outcome", outcomeId, ":", fit))
        dir.create(modelFolder)
        write.csv(fit, file.path(modelFolder, "Error.txt"))
      } else {
        betas <- coef(fit)
        intercept <- betas[1]
        betas <- betas[2:length(betas)]
        betas <- betas[betas != 0]
        if (length(betas) > 0){
          betas <- data.frame(beta = betas, id = as.numeric(attr(betas, "names")))
          betas <- merge(ff::as.ffdf(betas), covariateRef, by.x = "id", by.y = "covariateId")
          betas <- ff::as.ram(betas[, c("beta", "id", "covariateName")])
          betas <- betas[order(-abs(betas$beta)), ]
        }
        betas <- rbind(data.frame(beta = intercept, id = 0, covariateName = "(Intercept)", row.names = NULL), betas)
        prediction <- predict(fit)
        if (modelType == "survival") {
          # Convert Poisson-based prediction to rate for exponential distribution:
          prediction <- prediction/time
        }
        dir.create(modelFolder)
        saveRDS(prediction, file.path(modelFolder, "prediction.rds"))
        saveRDS(betas, file.path(modelFolder, "betas.rds"))
        haveModel <- TRUE
      }
    }
    if (haveModel) {
      result$modelFile <- file.path(modelFolder, "betas.rds")
      for (fxSizeIdx in 1:length(effectSizes)) {
        effectSize <- effectSizes[fxSizeIdx]
        if (effectSize != 1) {
          # When sampling, the expected RR size is the target RR, but the actual RR could be different due to
          # random error.  this code is redoing the sampling until actual RR is equal to the target RR.
          targetCount <- sum(outcomes$y) * (effectSize - 1)
          newOutcomeCounts <- 0
          if (modelType == "poisson") {
            multiplier <- 1
            ratios <- c()
            while (round(abs(sum(newOutcomeCounts) - targetCount)) > precision * targetCount){
              newOutcomeCounts <- rpois(length(prediction), multiplier * prediction * (effectSize - 1))
              newOutcomeCounts[newOutcomeCounts > time] <- time[newOutcomeCounts > time]
              ratios <- c(ratios, sum(newOutcomeCounts) / targetCount)
              if (length(ratios) == 100){
                multiplier <- multiplier * 1/mean(ratios)
                ratios <- c()
                writeLines(paste("Unable to achieve target RR using model as is. Adding multiplier of", multiplier, "to force target"))
              }
            }
            idx <- which(newOutcomeCounts != 0)
            temp <- data.frame(personId = outcomes$personId[idx],
                               cohortStartDate = outcomes$cohortStartDate[idx],
                               time = outcomes$time[idx],
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
            injectedRr <- 1 + (nrow(newOutcomes)/sum(outcomes$y))
            
            # Count outcomes during first episodes:
            newOutcomeCountsFirstExposure <- sum(newOutcomeCounts[outcomes$eraNumber == 1])
            injectedRrFirstExposure <- 1 + (newOutcomeCountsFirstExposure/firstExposureOutcomeCount)
          } else {
            correctedTargetCount <- targetCount
            ratios <- c()
            multiplier <- 1
            temp <- c()
            while (round(abs(sum(temp) - correctedTargetCount)) > precision * correctedTargetCount) {
              timeToEvent <- round(rexp(length(prediction), multiplier * prediction * (effectSize - 1)))
              temp <- timeToEvent <= time
              # Correct the target count for the fact that we're censoring after the first outcome:
              correctedTargetCount <- targetCount * (1-sum(time[timeToEvent <= time]-timeToEvent[timeToEvent <= time]) / sum(time))
              ratios <- c(ratios, sum(temp) / correctedTargetCount)
              if (length(ratios) == 100){
                multiplier <- multiplier * 1/mean(ratios)
                ratios <- c()
                writeLines(paste("Unable to achieve target RR using model as is. Adding multiplier of", multiplier, "to force target"))
              }
            }
            injectedRr <- effectSize*(sum(outcomeCounts$y) + sum(temp)) / (sum(outcomeCounts$y) + correctedTargetCount)
            newOutcomes <- data.frame(personId = outcomes$personId[temp],
                                      cohortStartDate = outcomes$cohortStartDate[temp],
                                      timeToEvent = timeToEvent[temp])
            # Count outcomes during first episodes:
            if (firstExposureOnly){
              injectedRrFirstExposure <- injectedRr
            } else {
              injectedRrFirstExposure <- injectedRr
              warning("Computing injected rate during first exposure only is not yet implemented for survival models")
            }
          }
        } else {
          newOutcomes <- data.frame()
          injectedRr <- 1
          injectedRrFirstExposure <- 1
        }
        writeLines(paste("Target RR =",
                         effectSize,
                         ", injected RR =",
                         injectedRr,
                         ", injected RR during first exposure only =",
                         injectedRrFirstExposure))
        
        newOutcomeId <- result$newOutcomeId[result$targetEffectSize == effectSize]
        # Inject new outcomes:
        if (nrow(newOutcomes) != 0) {
          newOutcomes$cohortStartDate <- newOutcomes$cohortStartDate + newOutcomes$timeToEvent
          newOutcomes$timeToEvent <- NULL
          newOutcomes$cohortDefinitionId <- newOutcomeId
          names(newOutcomes)[names(newOutcomes) == "personId"] <- "subjectId"
          outcomesToInjectFile <- file.path(workFolder, paste0("newOutcomes_e", exposureId, "_o", outcomeId, "_rr", effectSize , ".rds"))
          saveRDS(newOutcomes, outcomesToInjectFile)
          result$outcomesToInjectFile[result$targetEffectSize == effectSize] <- outcomesToInjectFile
        }
        idx <- result$targetEffectSize == effectSize
        result$trueEffectSize[idx] <- injectedRr
        result$trueEffectSizeFirstExposure[idx] <- injectedRrFirstExposure
        result$observedOutcomes[idx] <- sum(outcomes$y)
        result$injectedOutcomes[idx] <- nrow(newOutcomes)
      }
    }
  }
  return(result)
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

