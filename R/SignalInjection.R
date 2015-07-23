# @file SignalInjection.R
#
# Copyright 2015 Observational Health Data Sciences and Informatics
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
#' @param outcomeTable                     The table name that contains the outcome cohorts.  If
#'                                         outcomeTable <> CONDITION_OCCURRENCE, then expectation is
#'                                         outcomeTable has format of COHORT table:
#'                                         cohort_concept_id, SUBJECT_ID, COHORT_START_DATE,
#'                                         COHORT_END_DATE.
#' @param outcomeConditionTypeConceptIds   A list of TYPE_CONCEPT_ID values that will restrict
#'                                         condition occurrences.  Only applicable if outcomeTable =
#'                                         CONDITION_OCCURRENCE.
#' @param createOutputTable               Should the output table be created prior to inserting the
#'                                         outcomes? If TRUE and the tables already exists, it will
#'                                         first be deleted.
#' @param exposureOutcomePairs             A data frame with at least two columns:
#'                                         \itemize{
#'                                           \item {"exposureConceptId" containing the drug_concept_ID
#'                                                 or cohort_concept_id of the exposure variable}
#'                                           \item {"outcomeConceptId" containing the
#'                                                 condition_concept_ID or cohort_concept_id of the
#'                                                 outcome variable}
#'                                         }
#'
#' @param modelType                        Can be either "poisson" or "survival"
#' @param buildOutcomeModel                Should an outcome model be created to predict outcomes. New
#'                                         outcomes will be inserted based on the predicted
#'                                         probabilities according to this model, and this will help
#'                                         preserve the observed confounding when injecting signals.
#' @param firstExposureOnly                Should signals be injected only for the first exposure? (ie.
#'                                         assuming an acute effect)
#' @param washoutWindow                    Number of days at the start of observation for which no
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
#' @param tempFolder                       Path to a folder where intermediate data will be stored.                                     
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
                          outcomeTable = "condition_era",
                          outcomeConditionTypeConceptIds = c(),
                          createOutputTable = TRUE,
                          exposureOutcomePairs,
                          modelType = "poisson",
                          buildOutcomeModel = TRUE,
                          covariateSettings = createCovariateSettings(useCovariateDemographics = TRUE,
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
                                                                      useCovariateInteractionYear = FALSE,
                                                                      useCovariateInteractionMonth = FALSE,
                                                                      excludedCovariateConceptIds = c(),
                                                                      deleteCovariatesSmallCount = 100),
                          prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                          control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10),
                          firstExposureOnly = FALSE,
                          washoutWindow = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          firstOutcomeOnly = FALSE,
                          effectSizes = c(1, 1.25, 1.5, 2, 4),
                          precision = 0.01,
                          outputDatabaseSchema = cdmDatabaseSchema,
                          outputTable = "injected_outcomes",
                          outputConceptIdOffset = 1000,
                          tempFolder = "./SignalInjectionTemp") {
  if (min(effectSizes) < 1){
    stop("Effect sizes smaller than 1 are currently not supported")
  }
  
  
  result <- data.frame(exposureConceptId = rep(exposureOutcomePairs$exposureConceptId,
                                               each = length(effectSizes)),
                       outcomeConceptId = rep(exposureOutcomePairs$outcomeConceptId,
                                              each = length(effectSizes)),
                       targetEffectSize = rep(effectSizes, nrow(exposureOutcomePairs)),
                       newOutcomeConceptId = outputConceptIdOffset+(0:(nrow(exposureOutcomePairs)*length(effectSizes)-1)),
                       trueEffectSize = 0,
                       observedOutcomes = 0,
                       injectedOutcomes = 0)
  models <- list()
  outcomesToInject <- data.frame()
  outcomesCopySql <- c()  
  cdmDatabase <- strsplit(cdmDatabaseSchema, "\\.")[[1]][1]
  exposureConceptIds <- unique(exposureOutcomePairs$exposureConceptId)
  conn <- DatabaseConnector::connect(connectionDetails)
  for (exposureConceptId in exposureConceptIds) {
    # exposureConceptId = exposureConceptIds[1]
    writeLines(paste("\nProcessing exposure", exposureConceptId))
    outcomeConceptIds <- unique(exposureOutcomePairs$outcomeConceptId[exposureOutcomePairs$exposureConceptId ==
                                                                        exposureConceptId])
    renderedSql <- SqlRender::loadRenderTranslateSql("CreateExposedCohorts.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     oracleTempSchema = oracleTempSchema,
                                                     cdm_database = cdmDatabase,
                                                     exposure_concept_ids = exposureConceptId,
                                                     washout_window = washoutWindow,
                                                     exposure_database_schema = exposureDatabaseSchema,
                                                     exposure_table = exposureTable,
                                                     first_exposure_only = firstExposureOnly,
                                                     risk_window_start = riskWindowStart,
                                                     risk_window_end = riskWindowEnd,
                                                     add_exposure_days_to_end = addExposureDaysToEnd)
    
    writeLines("\nCreating risk windows")
    DatabaseConnector::executeSql(conn, renderedSql)
    
    writeLines("Extracting risk windows and outcome counts")
    exposureSql <- "SELECT cohort_concept_id, subject_id, cohort_start_date, DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS days_at_risk FROM #cohort_person"
    exposureSql <- SqlRender::translateSql(exposureSql,
                                           "sql server",
                                           connectionDetails$dbms,
                                           oracleTempSchema)$sql
    exposures <- DatabaseConnector::querySql.ffdf(conn, exposureSql)
    names(exposures) <- SqlRender::snakeCaseToCamelCase(names(exposures))
    names(exposures)[names(exposures) == "subjectId"] <- "personId"
    renderedSql <- SqlRender::loadRenderTranslateSql("GetOutcomes.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     oracleTempSchema = oracleTempSchema,
                                                     cdm_database = cdmDatabase,
                                                     outcome_database_schema = outcomeDatabaseSchema,
                                                     outcome_table = outcomeTable,
                                                     outcome_concept_ids = outcomeConceptIds,
                                                     outcome_condition_type_concept_ids = outcomeConditionTypeConceptIds,
                                                     first_outcome_only = firstOutcomeOnly)
    outcomeCounts <- DatabaseConnector::querySql.ffdf(conn, renderedSql)
    names(outcomeCounts) <- SqlRender::snakeCaseToCamelCase(names(outcomeCounts))
    names(outcomeCounts)[names(outcomeCounts) == "subjectId"] <- "personId"
    
    if (buildOutcomeModel) {
      covarDataFolder <- .createCovarDataFileName(tempFolder, exposureConceptId)
      if (file.exists(covarDataFolder)){
        writeLines("Loading covariates for the outcome model")
        ffbase::load.ffdf(covarDataFolder)
      } else {
        writeLines("Extracting covariates for the outcome model")
        covariates <- PatientLevelPrediction::getDbCovariateData(connection = conn,
                                                                 oracleTempSchema = oracleTempSchema,
                                                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                                                 useExistingCohortPerson = TRUE,
                                                                 cohortIds = exposureConceptId,
                                                                 covariateSettings = covariateSettings)
        covariateRef <- covariates$covariateRef
        covariates <- covariates$covariates
        exposures$rowId <- ff::ff(1:nrow(exposures))
        covariates <- merge(covariates, exposures, by = c("cohortStartDate", "personId"))
        outcomeCounts <- merge(exposures, outcomeCounts, by = c("cohortStartDate",
                                                                "personId"), all.x = TRUE)
        idx <- ffbase::is.na.ff(outcomeCounts$y)
        idx <- ffbase::ffwhich(idx, idx == TRUE)
        outcomeCounts$y <- ff::ffindexset(x = outcomeCounts$y,
                                          index = idx,
                                          value = ff::ff(0, length = length(idx), vmode = "double"))
        names(outcomeCounts)[names(outcomeCounts) == "daysAtRisk"] <- "time"
        outcomeCounts$time <- outcomeCounts$time + 1
        if (modelType == "survival") {
          # For survival, time is either the time to the end of the risk window, or the event
          outcomeCounts$timeToEvent <- outcomeCounts$timeToEvent + 1
          idx <- outcomeCounts$y != 0
          idx <- ffbase::ffwhich(idx, idx == TRUE)
          outcomeCounts$y <- ff::ffindexset(outcomeCounts$y, index = idx, value = ff::ff(as.double(1),
                                                                                         length = length(idx),
                                                                                         vmode = "double"))
          outcomeCounts$time <- ff::ffindexset(outcomeCounts$time,
                                               index = idx,
                                               value = outcomeCounts$timeToEvent[idx])
        }
        ffbase::save.ffdf(covariates, covariateRef, outcomeCounts, dir = covarDataFolder)
      }
      for (outcomeConceptId in outcomeConceptIds) {
        # outcomeConceptId = outcomeConceptIds[1]
        writeLines(paste("\nProcessing outcome", outcomeConceptId))
        outcomes <- subset(outcomeCounts,
                           outcomeConceptId == outcomeConceptId | is.na(outcomeConceptId))
        time <- ff::as.ram(outcomes$time)
        modelFolder <- .createModelFileName(tempFolder, exposureConceptId, outcomeConceptId)
        if (file.exists(modelFolder)){
          prediction <- readRDS(file.path(modelFolder, "prediction.rds"))
          betas <- readRDS(file.path(modelFolder, "betas.rds"))
        } else {
          # Note: for survival, using Poisson regression with 1 outcome and censored time as equivalent of
          # survival regression:
          cyclopsData <- Cyclops::convertToCyclopsData(outcomes, covariates, modelType = "pr", quiet = TRUE)
          fit <- Cyclops::fitCyclopsModel(cyclopsData, prior = prior, control = control)
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
        }
        models[[length(models) + 1]] <- betas
        names(models)[length(models)] <- paste(exposureConceptId,outcomeConceptId, sep="_")
        # plotCalibration(prediction, ff::as.ram(outcomes$y), ff::as.ram(outcomes$time))
        for (fxSizeIdx in 1:length(effectSizes)) {
          # fxSizeIdx <- 2
          effectSize <- effectSizes[fxSizeIdx]
          if (effectSize != 1) {
            # When sampling, the expected RR size is the target RR, but the actual RR could be different due to
            # random error. Not sure how important it is, but this code is redoing the sampling until actual RR
            # is equal to the target RR.
            targetCount <- sum(outcomeCounts$y) * (effectSize - 1)
            temp <- 0
            attempts <- 0
            if (modelType == "poisson") {
              while (abs(sum(temp) - targetCount) > precision * targetCount){
                temp <- rpois(length(prediction), prediction * (effectSize - 1))
                temp[temp > time] <- time[temp > time]
                attempts <- attempts + 1
                if (attempts == 100){
                  writeLines("Failed to achieve target RR")
                  break
                }
              }
              idx <- which(temp != 0)
              temp <- data.frame(personId = ff::as.ram(outcomes$personId[idx]),
                                 cohortStartDate = ff::as.ram(outcomes$cohortStartDate[idx]),
                                 time = ff::as.ram(outcomes$time[idx]),
                                 nOutcomes = temp[idx])
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
                                                                                         temp$time[i])
                  cursor <- cursor + nOutcomes
                }
              }
              injectedRr <- 1 + (nrow(newOutcomes)/sum(outcomeCounts$y))
            } else {
              # modelType == 'survival'
              correctedTargetCount <- targetCount
              attempts <- 0
              while (abs(sum(temp) - correctedTargetCount) > precision * correctedTargetCount) {
                timeToEvent <- round(rexp(length(prediction), prediction * (effectSize - 1)))
                temp <- timeToEvent <= time
                # Correct the target count for the fact that we're censoring after the first outcome:
                correctedTargetCount <- targetCount * (1-sum(time[timeToEvent <= time]-timeToEvent[timeToEvent <= time]) / sum(time))
                attempts <- attempts + 1
                if (attempts == 100){
                  writeLines("Failed to achieve target RR")
                  break
                }
              }
              injectedRr <- effectSize*(sum(outcomeCounts$y) + sum(temp)) / (sum(outcomeCounts$y) + correctedTargetCount)
              newOutcomes <- data.frame(personId = outcomes$personId[temp],
                                        cohortStartDate = outcomes$cohortStartDate[temp],
                                        timeToEvent = timeToEvent[temp])
            }
            writeLines(paste("Target RR =",
                             effectSize,
                             ", injected RR =",
                             injectedRr
            ))
          } else {
            # effectSize == 1
            newOutcomes <- data.frame()
            injectedRr <- 1
            writeLines(paste("Target RR = 1 , inserted RR = 1 (no signal inserted)"))
          }
          newOutcomeConceptId <- result$newOutcomeConceptId[result$targetEffectSize == effectSize & result$exposureConceptId == exposureConceptId & result$outcomeConceptId == outcomeConceptId]
          # Copy outcomes to output table:
          sql <- SqlRender::loadRenderTranslateSql("CopyOutcomes.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   cdm_database_schema = cdmDatabaseSchema,
                                                   outcome_database_schema = outcomeDatabaseSchema,
                                                   outcome_table = outcomeTable,
                                                   outcome_condition_type_concept_ids = outcomeConditionTypeConceptIds,
                                                   source_concept_id = outcomeConceptId,
                                                   target_concept_id = newOutcomeConceptId)
          outcomesCopySql <- c(outcomesCopySql, sql)
          
          # Inject new outcomes:
          if (nrow(newOutcomes) != 0) {
            newOutcomes$cohortStartDate <- newOutcomes$cohortStartDate + newOutcomes$timeToEvent
            newOutcomes$timeToEvent <- NULL
            newOutcomes$cohortConceptId <- newOutcomeConceptId
            names(newOutcomes) <- SqlRender::camelCaseToSnakeCase(names(newOutcomes))
            names(newOutcomes)[names(newOutcomes) == "person_id"] <- "subject_id"
            tableName <- paste(outputDatabaseSchema, outputTable, sep = ".")
            outcomesToInject <- rbind(outcomesToInject, newOutcomes)
          }
          idx <- result$exposureConceptId == exposureConceptId & result$outcomeConceptId == outcomeConceptId &
            result$targetEffectSize == effectSize
          result$trueEffectSize[idx] <- injectedRr
          result$observedOutcomes[idx] <- sum(outcomeCounts$y)
          result$injectedOutcomes[idx] <- nrow(newOutcomes)
        }
      }
    }
  }
  ### REMOVE THIS ###
  #saveRDS(outcomesToInject, "s:/temp/outcomesToInject.rds")
  #saveRDS(outcomesCopySql, "s:/temp/outcomesCopySql.rds")
  ###
  
  writeLines("Inserting outcomes into database")
  DatabaseConnector::insertTable(conn, "#temp_outcomes", outcomesToInject, TRUE, TRUE, TRUE, oracleTempSchema)
  
  if (createOutputTable){
    sql <- "IF OBJECT_ID('@output_database_schema.@output_table', 'U') IS NOT NULL\nDROP TABLE @output_database_schema.@output_table;\n"
    sql <- paste(sql, "SELECT cohort_concept_id, cohort_start_date, cohort_end_date, subject_id INTO @output_database_schema.@output_table FROM (\n")
  } else{
    sql <- "INSERT INTO @output_database_schema.@output_table (cohort_concept_id, cohort_start_date, cohort_end_date, subject_id)\n";
  }
  sql <- paste(sql, "SELECT cohort_concept_id, cohort_start_date, NULL AS cohort_end_date, subject_id FROM #temp_outcomes UNION ALL\n")
  sql <- paste(sql, paste(outcomesCopySql, collapse = " UNION ALL "))
  if (createOutputTable){
    sql <- paste(sql, ") temp;")
  } else {
    sql <- paste(sql, ";")
  }
  sql <- SqlRender::renderSql(sql, output_database_schema = outputDatabaseSchema, output_table = outputTable)$sql
  sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
  DatabaseConnector::executeSql(conn, sql)
  
  sql <- "TRUNCATE TABLE #cohort_person; DROP TABLE #cohort_person; TRUNCATE TABLE #temp_outcomes; DROP TABLE #temp_outcomes;"
  sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
  DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  RJDBC::dbDisconnect(conn)
  resultList <- list(summary = result, models = models)
  summaryFile <- .createSummaryFileName(tempFolder)
  saveRDS(resultList, summaryFile)
  return(resultList)
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

.createModelFileName <- function(folder,
                                 exposureConceptId,
                                 outcomeConceptId) {
  name <- paste("model_e", exposureConceptId, "_o", outcomeConceptId, sep = "")
  return(file.path(folder, name))
}

.createCovarDataFileName <- function(folder,
                                     exposureConceptId) {
  name <- paste("data_e", exposureConceptId, sep = "")
  return(file.path(folder, name))
}

.createSummaryFileName <- function(folder) {
  name <- "summary"
  name <- paste(name, ".rds", sep = "")
  return(file.path(folder, name))
}

