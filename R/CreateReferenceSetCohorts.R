# Copyright 2019 Observational Health Data Sciences and Informatics
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

#' Create cohorts used in a reference set.
#'
#' @details
#' This function will create the outcomes of interest and nesting cohorts referenced in the various
#' reference sets. The outcomes of interest are derives using information like diagnoses, procedures,
#' and drug prescriptions. The outcomes are stored in a table on the database server.
#'
#' @param connectionDetails       An R object of type \code{ConnectionDetails} created using the
#'                                function \code{createConnectionDetails} in the
#'                                \code{DatabaseConnector} package.
#' @param oracleTempSchema        Should be used in Oracle to specify a schema where the user has write
#'                                priviliges for storing temporary tables.
#' @param cdmDatabaseSchema       A database schema containing health care data in the OMOP Commond
#'                                Data Model. Note that for SQL Server, botth the database and schema
#'                                should be specified, e.g. 'cdm_schema.dbo'
#' @param outcomeDatabaseSchema   The database schema where the target outcome table is located. Note
#'                                that for SQL Server, both the database and schema should be
#'                                specified, e.g. 'cdm_schema.dbo'
#' @param outcomeTable            The name of the table where the outcomes will be stored.
#' @param nestingDatabaseSchema   (For the OHDSI Methods Benchmark only) The database schema where the
#'                                nesting outcome table is located. Note that for SQL Server, both the
#'                                database and schema should be specified, e.g. 'cdm_schema.dbo'.
#' @param nestingTable            (For the OHDSI Methods Benchmark only) The name of the table where
#'                                the nesting cohorts will be stored.
#' @param referenceSet            The name of the reference set for which outcomes need to be created.
#'                                Currently supported are "omopReferenceSet", "euadrReferenceSet", and
#'                                "ohdsiMethodsBenchmark".
#'
#' @export
createReferenceSetCohorts <- function(connectionDetails,
                                      oracleTempSchema = NULL,
                                      cdmDatabaseSchema,
                                      outcomeDatabaseSchema = cdmDatabaseSchema,
                                      outcomeTable = "outcomes",
                                      nestingDatabaseSchema = cdmDatabaseSchema,
                                      nestingTable = "nesting",
                                      referenceSet = "ohdsiMethodsBenchmark") {
  
  if (referenceSet == "omopReferenceSet") {
    ParallelLogger::logInfo("Generating HOIs for the OMOP reference set")
    renderedSql <- SqlRender::loadRenderTranslateSql("CreateOmopHois.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     outcome_database_schema = outcomeDatabaseSchema,
                                                     outcome_table = outcomeTable)
    conn <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(conn, renderedSql)
    ParallelLogger::logInfo("Done")
    DatabaseConnector::disconnect(conn)
  } else if (referenceSet == "euadrReferenceSet") {
    ParallelLogger::logInfo("Generating HOIs for the EU-ADR reference set")
    # TODO: add code for creating the EU-ADR HOIs
  } else if (referenceSet == "ohdsiMethodsBenchmark") {
    ParallelLogger::logInfo("Generating HOIs and nesting cohorts for the OHDSI Methods Benchmark")
    if (outcomeDatabaseSchema == nestingDatabaseSchema && outcomeTable == nestingTable)
      stop("Outcome and nesting cohorts cannot be created in the same table")
    createOhdsiNegativeControlCohorts(connectionDetails = connectionDetails,
                                      cdmDatabaseSchema = cdmDatabaseSchema,
                                      outcomeDatabaseSchema = outcomeDatabaseSchema,
                                      outcomeTable = outcomeTable,
                                      nestingDatabaseSchema = nestingDatabaseSchema,
                                      nestingTable = nestingTable,
                                      oracleTempSchema = oracleTempSchema)
  } else {
    stop(paste("Unknow reference set:", referenceSet))
  }
}

createOhdsiNegativeControlCohorts <- function(connectionDetails,
                                              cdmDatabaseSchema,
                                              outcomeDatabaseSchema,
                                              outcomeTable,
                                              nestingDatabaseSchema,
                                              nestingTable,
                                              oracleTempSchema) {
  ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                               package = "MethodEvaluation"))
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateCohortTable.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = outcomeDatabaseSchema,
                                           cohort_table = outcomeTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  sql <- SqlRender::loadRenderTranslateSql(sqlFilename = "CreateCohortTable.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cohort_database_schema = nestingDatabaseSchema,
                                           cohort_table = nestingTable)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  cohortsToCreate <- data.frame(name = c("acute_pancreatitis", "gi_bleed", "stroke", "ibd"),
                                id = c(1, 2, 3, 4))
  for (i in 1:nrow(cohortsToCreate)) {
    ParallelLogger::logInfo(paste("Creating outcome:", cohortsToCreate$name[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate$name[i], ".sql"),
                                             packageName = "MethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             vocabulary_database_schema = cdmDatabaseSchema,
                                             target_database_schema = outcomeDatabaseSchema,
                                             target_cohort_table = outcomeTable,
                                             target_cohort_id = cohortsToCreate$id[i])
    DatabaseConnector::executeSql(connection, sql)
  }
  ParallelLogger::logInfo("Creating other negative control outcomes")
  negativeControlIds <- ohdsiNegativeControls$outcomeId[ohdsiNegativeControls$outcomeId > 4]
  sql <- SqlRender::loadRenderTranslateSql("NegativeControls.sql",
                                           "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           target_database_schema = outcomeDatabaseSchema,
                                           target_cohort_table = outcomeTable,
                                           outcome_ids = negativeControlIds)
  DatabaseConnector::executeSql(connection, sql)
  
  ParallelLogger::logInfo("Creating nesting cohorts")
  nestingIds <- ohdsiNegativeControls$nestingId
  sql <- SqlRender::loadRenderTranslateSql("NestingCohorts.sql",
                                           "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           target_database_schema = nestingDatabaseSchema,
                                           target_cohort_table = nestingTable,
                                           nesting_ids = nestingIds)
  DatabaseConnector::executeSql(connection, sql)
  
  ParallelLogger::logInfo("Counting cohorts")
}


#' Synthesize positive controls for reference set
#'
#' @details
#' This function will synthesize positive controls for a given reference set based on the real
#' negative controls. Data from the database will be used to fit outcome models for each negative
#' control outcome, and these models will be used to sample additional synthetic outcomes during
#' eposure to increase the true hazard ratio.
#' The positive control outcome cohorts will be stored in the same database table as the negative
#' control outcome cohorts.
#' A summary file will be created listing all positive and negative controls. This list should then be
#' used as input for the method under evaluation.
#'
#' @param connectionDetails        An R object of type \code{ConnectionDetails} created using the
#'                                 function \code{createConnectionDetails} in the
#'                                 \code{DatabaseConnector} package.
#' @param oracleTempSchema         Should be used in Oracle to specify a schema where the user has
#'                                 write priviliges for storing temporary tables.
#' @param cdmDatabaseSchema        A database schema containing health care data in the OMOP Commond
#'                                 Data Model. Note that for SQL Server, botth the database and schema
#'                                 should be specified, e.g. 'cdm_schema.dbo'
#' @param outcomeDatabaseSchema    The database schema where the target outcome table is located. Note
#'                                 that for SQL Server, both the database and schema should be
#'                                 specified, e.g. 'cdm_schema.dbo'
#' @param outcomeTable             The name of the table where the outcomes will be stored.
#' @param exposureDatabaseSchema   The name of the database schema that is the location where the
#'                                 exposure data used to define the exposure cohorts is available.  If
#'                                 exposureTable = DRUG_ERA, exposureDatabaseSchema is not used and
#'                                 assumed to be cdmDatabaseSchema.  Requires read permissions to this
#'                                 database.
#' @param exposureTable            The tablename that contains the exposure cohorts.  If exposureTable
#'                                 <> DRUG_ERA, then expectation is exposureTable has format of COHORT
#'                                 table: COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                 COHORT_END_DATE.
#' @param referenceSet             The name of the reference set for which positive controls need to be
#'                                 synthesized. Currently supported are "ohdsiMethodsBenchmark".
#' @param maxCores                 How many parallel cores should be used? If more cores are made
#'                                 available this can speed up the analyses.
#' @param workFolder               Name of local folder to place intermediary results; make sure to use
#'                                 forward slashes (/). Do not use a folder on a network drive since
#'                                 this greatly impacts performance.
#' @param summaryFileName          The name of the CSV file where to store the summary of the final set
#'                                 of positive and negative controls.
#'
#' @export
synthesizeReferenceSetPositiveControls <- function(connectionDetails,
                                                   cdmDatabaseSchema,
                                                   oracleTempSchema = NULL,
                                                   outcomeDatabaseSchema = cdmDatabaseSchema,
                                                   outcomeTable = "cohort",
                                                   exposureDatabaseSchema = cdmDatabaseSchema,
                                                   exposureTable = "drug_era",
                                                   referenceSet = "ohdsiMethodsBenchmark",
                                                   maxCores = 1,
                                                   workFolder,
                                                   summaryFileName = file.path(workFolder, "allControls.csv")) {
  if (referenceSet != "ohdsiMethodsBenchmark") {
    stop("Currently only supporting positive control synthesis for the ohdsiMethodsBenchmark reference set")
  }
  injectionFolder <- file.path(workFolder, "SignalInjection")
  if (!file.exists(injectionFolder))
    dir.create(injectionFolder)
  
  injectionSummaryFile <- file.path(workFolder, "injectionSummary.rds")
  if (!file.exists(injectionSummaryFile)) {
    ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                                 package = "MethodEvaluation"))
    exposureOutcomePairs <- data.frame(exposureId = ohdsiNegativeControls$targetId,
                                       outcomeId = ohdsiNegativeControls$outcomeId)
    exposureOutcomePairs <- unique(exposureOutcomePairs)
    
    prior <- Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
    
    control <- Cyclops::createControl(cvType = "auto",
                                      startingVariance = 0.01,
                                      noiseLevel = "quiet",
                                      cvRepetitions = 1,
                                      threads = min(c(10, maxCores)))
    
    covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsAgeGroup = TRUE,
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
                                                                    endDays = 0)
    
    result <- injectSignals(connectionDetails,
                            cdmDatabaseSchema = cdmDatabaseSchema,
                            oracleTempSchema = oracleTempSchema,
                            exposureDatabaseSchema = exposureDatabaseSchema,
                            exposureTable = exposureTable,
                            outcomeDatabaseSchema = outcomeDatabaseSchema,
                            outcomeTable = outcomeTable,
                            outputDatabaseSchema = outcomeDatabaseSchema,
                            outputTable = outcomeTable,
                            createOutputTable = FALSE,
                            outputIdOffset = 10000,
                            exposureOutcomePairs = exposureOutcomePairs,
                            firstExposureOnly = FALSE,
                            firstOutcomeOnly = TRUE,
                            removePeopleWithPriorOutcomes = TRUE,
                            modelType = "survival",
                            washoutPeriod = 365,
                            riskWindowStart = 0,
                            riskWindowEnd = 0,
                            addExposureDaysToEnd = TRUE,
                            effectSizes = c(1.5, 2, 4),
                            precision = 0.01,
                            prior = prior,
                            control = control,
                            maxSubjectsForModel = 250000,
                            minOutcomeCountForModel = 100,
                            minOutcomeCountForInjection = 25,
                            workFolder = injectionFolder,
                            modelThreads = max(1, round(maxCores/8)),
                            generationThreads = min(6, maxCores),
                            covariateSettings = covariateSettings)
    saveRDS(result, injectionSummaryFile)
  }
  ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                               package = "MethodEvaluation"))
  injectedSignals <- readRDS(injectionSummaryFile)
  injectedSignals$targetId <- injectedSignals$exposureId
  injectedSignals <- merge(injectedSignals, ohdsiNegativeControls)
  injectedSignals <- injectedSignals[injectedSignals$trueEffectSize != 0, ]
  injectedSignals$outcomeName <- paste0(injectedSignals$outcomeName,
                                        ", RR=",
                                        injectedSignals$targetEffectSize)
  injectedSignals$oldOutcomeId <- injectedSignals$outcomeId
  injectedSignals$outcomeId <- injectedSignals$newOutcomeId
  ohdsiNegativeControls$targetEffectSize <- 1
  ohdsiNegativeControls$trueEffectSize <- 1
  ohdsiNegativeControls$trueEffectSizeFirstExposure <- 1
  ohdsiNegativeControls$oldOutcomeId <- ohdsiNegativeControls$outcomeId
  allControls <- rbind(ohdsiNegativeControls, injectedSignals[, names(ohdsiNegativeControls)])
  exposureOutcomes <- data.frame()
  exposureOutcomes <- rbind(exposureOutcomes, data.frame(exposureId = allControls$targetId,
                                                         outcomeId = allControls$outcomeId))
  exposureOutcomes <- rbind(exposureOutcomes, data.frame(exposureId = allControls$comparatorId,
                                                         outcomeId = allControls$outcomeId))
  exposureOutcomes <- unique(exposureOutcomes)
  mdrr <- computeMdrr(connectionDetails = connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      oracleTempSchema = oracleTempSchema,
                      exposureOutcomePairs = exposureOutcomes,
                      exposureDatabaseSchema = exposureDatabaseSchema,
                      exposureTable = exposureTable,
                      outcomeDatabaseSchema = outcomeDatabaseSchema,
                      outcomeTable = outcomeTable,
                      cdmVersion = "5")
  allControls <- merge(allControls, data.frame(targetId = mdrr$exposureId,
                                               outcomeId = mdrr$outcomeId,
                                               mdrrTarget = mdrr$mdrr))
  allControls <- merge(allControls, data.frame(comparatorId = mdrr$exposureId,
                                               outcomeId = mdrr$outcomeId,
                                               mdrrComparator = mdrr$mdrr), all.x = TRUE)
  write.csv(allControls, summaryFileName, row.names = FALSE)
}
