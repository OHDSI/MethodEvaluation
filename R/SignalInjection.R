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
#'                                         has format of COHORT table: COHORT_DEFINITION_ID,
#'                                         SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema            The name of the database schema that is the location where
#'                                         the data used to define the outcome cohorts is available.
#'                                         If exposureTable = CONDITION_ERA, exposureDatabaseSchema is
#'                                         not used by assumed to be cdmSchema.  Requires read
#'                                         permissions to this database.
#' @param outcomeTable                     The table name that contains the outcome cohorts.  If
#'                                         outcomeTable <> CONDITION_OCCURRENCE, then expectation is
#'                                         outcomeTable has format of COHORT table:
#'                                         COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                         COHORT_END_DATE.
#' @param outcomeConditionTypeConceptIds   A list of TYPE_CONCEPT_ID values that will restrict
#'                                         condition occurrences.  Only applicable if outcomeTable =
#'                                         CONDITION_OCCURRENCE.
#' @param outputDatabaseSchema             The name of the database schema that is the location of the
#'                                         table containing the new outcomesRequires write permissions
#'                                         to this database.
#' @param outputTable                      The table name that contains the outcome cohorts generated
#'                                         during signal injection.
#' @param createOutputTable                Should the output table be created prior to inserting the
#'                                         outcomes? If TRUE and the table already exists, it will
#'                                         first be deleted.
#' @param exposureOutcomePairs             A data frame with at least two columns:
#'                                         \itemize{
#'                                           \item {"exposureConceptId" containing the drug_concept_ID
#'                                                 or cohort_definition_id of the exposure variable}
#'                                           \item {"outcomeConceptId" containing the
#'                                                 condition_concept_ID or cohort_definition_id of the
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
#'
#' @return
#' A data.frame listing all the drug-pairs in combination with effect sizes, the outcome concept IDs
#' that were generated for each outcome-effect size combination, and, the real inserted effect size
#' (might be different from the requested effect size because of sampling error).#'
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
                          outputDatabaseSchema = cdmDatabaseSchema,
                          outputTable = "generated_outcomes",
                          createOutputTable = TRUE,
                          exposureOutcomePairs,
                          modelType = "poisson",
                          buildOutcomeModel = TRUE,
                          firstExposureOnly = FALSE,
                          washoutWindow = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          firstOutcomeOnly = FALSE,
                          effectSizes = c(1, 1.25, 1.5, 2, 4, 8)) {
  if (min(effectSizes) < 1)
    stop("Effect sizes smaller than 1 are currently not supported")

  result <- data.frame(exposureConceptId = rep(exposureOutcomePairs$exposureConceptId,
                                               each = length(effectSizes)),
                       outcomeConceptId = rep(exposureOutcomePairs$outcomeConceptId,
                                              each = length(effectSizes)),
                       effectSize = rep(effectSizes, nrow(exposureOutcomePairs)),
                       newOutcomeConceptId = 0,
                       trueEffectSize = 0,
                       observedOutcomes = 0,
                       injectedOutcomes = 0)


  cdmDatabase <- strsplit(cdmDatabaseSchema, "\\.")[[1]][1]
  exposureConceptIds <- unique(exposureOutcomePairs$exposureConceptId)
  conn <- DatabaseConnector::connect(connectionDetails)

  if (createOutputTable) {
    sql <- SqlRender::loadRenderTranslateSql("CreateSignalInjectionOutputTable.sql",
                                             packageName = "MethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             output_database_schema = outputDatabaseSchema,
                                             output_table = outputTable)
    DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }

  for (exposureConceptId in exposureConceptIds) {
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
    exposureSql <- "SELECT cohort_definition_id, subject_id, cohort_start_date, DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS days_at_risk FROM #cohort_person"
    exposureSql <- SqlRender::translateSql(exposureSql,
                                           "sql server",
                                           connectionDetails$dbms,
                                           oracleTempSchema)$sql
    exposures <- DatabaseConnector::querySql.ffdf(conn, exposureSql)
    names(exposures) <- SqlRender::snakeCaseToCamelCase(names(exposures))
    names(exposures)[names(exposures) == "subjectId"] <- "personId"

    exposure_outcome_pairs <- data.frame(exposure_concept_id = exposureOutcomePairs$exposureConceptId,
                                         outcome_concept_id = exposureOutcomePairs$outcomeConceptId)
    DatabaseConnector::dbInsertTable(conn,
                                     "#exposure_outcome_pairs",
                                     exposure_outcome_pairs,
                                     TRUE,
                                     TRUE,
                                     TRUE,
                                     oracleTempSchema)
    renderedSql <- SqlRender::loadRenderTranslateSql("GetOutcomes.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     oracleTempSchema = oracleTempSchema,
                                                     cdm_database = cdmDatabase,
                                                     outcome_database_schema = outcomeDatabaseSchema,
                                                     outcome_table = outcomeTable,
                                                     outcome_concept_ids = outcomeConceptIds,
                                                     outcome_condition_type_concept_ids = outcomeConditionTypeConceptIds)
    outcomeCounts <- DatabaseConnector::querySql.ffdf(conn, renderedSql)
    names(outcomeCounts) <- SqlRender::snakeCaseToCamelCase(names(outcomeCounts))
    names(outcomeCounts)[names(outcomeCounts) == "subjectId"] <- "personId"

    if (buildOutcomeModel) {
      writeLines("Extracting covariates for the outcome model")
      covariates <- CohortMethod::getDbCovariates(connection = conn,
                                                  oracleTempSchema = oracleTempSchema,
                                                  cdmDatabaseSchema = cdmDatabaseSchema,
                                                  useExistingCohortPerson = TRUE,
                                                  cohortConceptIds = exposureConceptId,
                                                  useCovariateDemographics = TRUE,
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
                                                  useCovariateObservationBelow = TRUE,
                                                  useCovariateObservationAbove = TRUE,
                                                  useCovariateObservationCount365d = TRUE,
                                                  useCovariateConceptCounts = TRUE,
                                                  useCovariateRiskScores = TRUE,
                                                  useCovariateInteractionYear = FALSE,
                                                  useCovariateInteractionMonth = FALSE,
                                                  excludedCovariateConceptIds = c(),
                                                  deleteCovariatesSmallCount = 100)
      covariates <- covariates$covariates
      exposures$rowId <- ff::ff(1:nrow(exposures))
      covariates <- merge(covariates, exposures, by = c("cohortStartDate", "personId"))
      covariates <- covariates[ff::ffdforder(covariates[c("rowId")]), ]
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
        # For survival, time is the either the time to the end of the risk window, or the event
        outcomeCounts$timeToEvent <- outcomeCounts$timeToEvent + 1
        idx <- outcomeCounts$y != 0
        idx <- ffbase::ffwhich(idx, idx == TRUE)
        outcomeCounts$y <- ff::ffindexset(outcomeCounts$y, index = idx, value = ff::ff(as.double(1),
                                                                                       length = length(idx),
                                                                                       vmode = "double"))
        outcomeCounts$time <- frf::ffindexset(outcomeCounts$time,
                                              index = idx,
                                              value = outcomeCounts$timeToEvent[idx])
      }
      for (outcomeConceptId in outcomeConceptIds) {
        writeLines(paste("\nProcessing outcome", outcomeConceptId))
        outcomes <- subset(outcomeCounts,
                           outcomeConceptId == outcomeConceptId | is.na(outcomeConceptId))
        # Note: for survival, using Poisson regression with 1 outcome and censored time as equivalent of
        # survival regression:
        cyclopsData <- convertToCyclopsData(outcomes, covariates, modelType = "pr")
        prior <- createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
        control <- createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet")
        fit <- fitCyclopsModel(cyclopsData, prior = prior, control = control)
        time <- ff::as.ram(outcomes$time)
        prediction <- predict(fit)
        if (modelType == "survival") {
          # Convert Poisson-based prediction to rate for exponential distribution:
          prediction <- prediction/time

        }
        # plotCalibration(prediction, ff::as.ram(outcomes$y), ff::as.ram(outcomes$time))
        for (fxSizeIdx in 1:length(effectSizes)) {
          effectSize <- effectSizes[fxSizeIdx]
          if (effectSize != 1) {
          # When sampling, the expected RR size is the target RR, but the actual RR could be different due to
          # random error. Not sure how important it is, but this code is redoing the sampling until actual RR
          # is equal to the target RR.
          precision <- 0.01
          targetCount <- sum(outcomeCounts$y) * (effectSize - 1)
          temp <- 0
          if (modelType == "poisson") {
            while (abs(sum(temp) - targetCount) > min(precision * targetCount, 1)) {
            temp <- rpois(length(prediction), prediction * (effectSize - 1))
            temp[temp > time] <- time[temp > time]
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

          } else {
            # modelType == 'survival'
            idx <- outcomes$y == 0
            idx <- ff::as.ram(ffbase::ffwhich(idx, idx == TRUE))
            prediction <- prediction[idx]
            while (abs(sum(temp) - targetCount) > min(precision * targetCount, 1)) {
            timeToEvent <- round(rexp(length(prediction), prediction * (effectSize - 1)))
            temp <- timeToEvent <= time[idx]
            }
            newOutcomes <- data.frame(personId = outcomes$personId[idx[temp]],
                                      cohortStartDate = outcomes$cohortStartDate[idx[temp]],
                                      timeToEvent = timeToEvent[temp])
          }


          writeLines(paste("Target RR =",
                           effectSize,
                           ", inserted RR =",
                           1 + (nrow(newOutcomes)/sum(outcomeCounts$y))))
          } else {
          # effectSize == 1
          newOutcomes <- data.frame()
          writeLines(paste("Target RR = 1 , inserted RR = 1 (no signal inserted)"))
          }
          newOutcomeConceptId <- outcomeConceptId * 100 + fxSizeIdx

          writeLines("\nWriting outcome to table")
          # Copy outcomes to output table:
          sql <- SqlRender::loadRenderTranslateSql("CopyOutcomes.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   cdm_database = cdmDatabase,
                                                   outcome_database_schema = outcomeDatabaseSchema,
                                                   outcome_table = outcomeTable,
                                                   outcome_condition_type_concept_ids = outcomeConditionTypeConceptIds,
                                                   output_database_schema = outputDatabaseSchema,
                                                   output_table = outputTable,
                                                   source_concept_id = outcomeConceptId,
                                                   target_concept_id = newOutcomeConceptId)
          DatabaseConnector::executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)

          # Inject new outcomes:
          if (nrow(newOutcomes) != 0) {
          newOutcomes$cohortStartDate <- newOutcomes$cohortStartDate + newOutcomes$timeToEvent
          newOutcomes$timeToEvent <- NULL
          newOutcomes$cohortConceptId <- newOutcomeConceptId
          names(newOutcomes) <- SqlRender::camelCaseToSnakeCase(names(newOutcomes))
          names(newOutcomes)[names(newOutcomes) == "person_id"] <- "subject_id"
          tableName <- paste(outputDatabaseSchema, outputTable, sep = ".")
          DatabaseConnector::dbInsertTable(conn, tableName, newOutcomes, FALSE, FALSE, FALSE)
          }
          idx <- result$exposureConceptId == exposureConceptId & result$outcomeConceptId == outcomeConceptId &
          result$effectSize == effectSize
          result$newOutcomeConceptId[idx] <- newOutcomeConceptId
          result$trueEffectSize[idx] <- 1 + (nrow(newOutcomes)/sum(outcomeCounts$y))
          result$observedOutcomes[idx] <- sum(outcomeCounts$y)
          result$injectedOutcomes[idx] <- nrow(newOutcomes)
        }
      }
    }
    # TODO: remove cohort_person temp table
  }
  dummy <- RJDBC::dbDisconnect(conn)
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
