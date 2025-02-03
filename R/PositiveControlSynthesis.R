# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Synthesize positive controls
#'
#' @details
#' This function will insert additional outcomes for a given set of drug-outcome pairs. It is assumed
#' that these drug-outcome pairs represent negative controls, so the true relative risk before
#' inserting any outcomes should be 1. There are two models for inserting the outcomes during the
#' specified risk window of the drug: a Poisson model assuming multiple outcomes could occurr during a
#' single exposure, and a survival model considering only one outcome per exposure.
#' It is possible to use bulk import to insert the generated outcomes in the database. This requires
#' the environmental variable 'USE_MPP_BULK_LOAD' to be set to 'TRUE'. See \code{
#' ?DatabaseConnector::insertTable} for details on how to configure the bulk upload.
#'
#' @param connectionDetails               An R object of type \code{ConnectionDetails} created using
#'                                        the function \code{createConnectionDetails} in the
#'                                        \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema               Name of database schema that contains OMOP CDM and
#'                                        vocabulary.
#' @param oracleTempSchema    DEPRECATED: use `tempEmulationSchema` instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @param exposureDatabaseSchema          The name of the database schema that is the location where
#'                                        the exposure data used to define the exposure cohorts is
#'                                        available.  If exposureTable = DRUG_ERA,
#'                                        exposureDatabaseSchema is not used by assumed to be
#'                                        cdmSchema.  Requires read permissions to this database.
#' @param exposureTable                   The table name that contains the exposure cohorts.  If
#'                                        exposureTable <> DRUG_ERA, then expectation is exposureTable
#'                                        has format of COHORT table: cohort_concept_id, SUBJECT_ID,
#'                                        COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema           The name of the database schema that is the location where
#'                                        the data used to define the outcome cohorts is available. If
#'                                        exposureTable = CONDITION_ERA, exposureDatabaseSchema is not
#'                                        used by assumed to be cdmSchema.  Requires read permissions
#'                                        to this database.
#' @param outcomeTable                    The table name that contains the outcome cohorts. When the
#'                                        table name is not CONDITION_ERA This table is expected to
#'                                        have the same format as the COHORT table: SUBJECT_ID,
#'                                        COHORT_START_DATE, COHORT_END_DATE, COHORT_CONCEPT_ID (CDM
#'                                        v4) or COHORT_DEFINITION_ID (CDM v5 and higher).
#' @param createOutputTable               Should the output table be created prior to inserting the
#'                                        outcomes? If TRUE and the tables already exists, it will
#'                                        first be deleted. If FALSE, the table is assumed to exist and
#'                                        the outcomes will be inserted. Any existing outcomes with the
#'                                        same IDs will first be deleted.
#' @param outputIdOffset                  What should be the first new outcome ID that is to be
#'                                        created?
#' @param exposureOutcomePairs            A data frame with at least two columns:
#'                                        \itemize{
#'                                          \item {"exposureId" containing the drug_concept_ID or
#'                                                cohort_concept_id of the exposure variable}
#'                                          \item {"outcomeId" containing the condition_concept_ID or
#'                                                cohort_concept_id of the outcome variable}
#'                                        }
#'
#'
#' @param modelType                       Can be either "poisson" or "survival"
#' @param minOutcomeCountForModel         Minimum number of outcome events required to build a model.
#' @param minOutcomeCountForInjection     Minimum number of outcome events required to inject a signal.
#' @param minModelCount                   Minimum number of negative controls having enough outcomes to
#'                                        fit an outcome model.
#' @param covariateSettings               An object of type \code{covariateSettings} as created using
#'                                        the \code{createCovariateSettings} function in the
#'                                        \code{FeatureExtraction} package.
#' @param prior                           The prior used to fit the outcome model. See
#'                                        \code{\link[Cyclops]{createPrior}} for details.
#' @param control                         The control object used to control the cross-validation used
#'                                        to determine the hyperparameters of the prior (if
#'                                        applicable). See \code{\link[Cyclops]{createControl}} for
#'                                        details.
#' @param precision                       The allowed ratio between target and injected signal size.
#' @param firstExposureOnly               Should signals be injected only for the first exposure? (ie.
#'                                        assuming an acute effect)
#' @param washoutPeriod                   Number of days at the start of observation for which no
#'                                        signals will be injected, but will be used to determine
#'                                        whether exposure or outcome is the first one, and for
#'                                        extracting covariates to build the outcome model.
#' @param riskWindowStart                 The start of the risk window relative to the start of the
#'                                        exposure (in days). When 0, risk is assumed to start on the
#'                                        first day of exposure.
#' @param riskWindowEnd                   The end of the risk window (in days) relative to the endAnchor.
#' @param endAnchor                       The anchor point for the end of the risk window. Can be
#'                                        "cohort start" or "cohort end".
#' @param minDaysAtRisk                   The minimum days at risk. Any risk period shorter than this 
#'                                        will be removed before model fitting and signal injection. 
#'                                        This is especially helpful when your data contains many 
#'                                        exposures where start date = end date, and we want to ignore
#'                                        those.
#' @param addIntentToTreat                If true, the signal will not only be injected in the primary
#'                                        time at risk, but also after the time at risk (up until the
#'                                        obseration period end). In both time periods, the target
#'                                        effect size will be enforced. This allows the same positive
#'                                        control synthesis to be used in both on treatment and
#'                                        intent-to-treat analysis variants. However, this will
#'                                        preclude the controls to be used in self-controlled designs
#'                                        that consider the time after exposure. Requires
#'                                        \code{firstExposureOnly = TRUE}.
#' @param firstOutcomeOnly                Should only the first outcome per person be considered when
#'                                        modeling the outcome?
#' @param removePeopleWithPriorOutcomes   Remove people with prior outcomes?
#' @param maxSubjectsForModel             Maximum number of people used to fit an outcome model.
#' @param effectSizes                     A numeric vector of effect sizes that should be inserted.
#' @param outputDatabaseSchema            The name of the database schema that is the location of the
#'                                        tables containing the new outcomesRequires write permissions
#'                                        to this database.
#' @param outputTable                     The name of the table names that will contain the generated
#'                                        outcome cohorts.
#' @param workFolder                      Path to a folder where intermediate data will be stored.
#' @param cdmVersion                      DEPRECATED. This argument is ignored.
#' @param modelThreads                    Number of parallel threads to use when fitting outcome
#'                                        models.
#' @param generationThreads               Number of parallel threads to use when generating outcomes.
#'
#' @references
#' Schuemie MJ, Hripcsak G, Ryan PB, Madigan D, Suchard MA. Empirical confidence interval calibration
#' for population-level effect estimation studies in observational healthcare data. Proc Natl Acad Sci
#' U S A. 2018 Mar 13;115(11):2571-2577.
#'
#' @return
#' A data.frame listing all the drug-pairs in combination with requested effect sizes and the real
#' inserted effect size (might be different from the requested effect size because of sampling error).
#'
#' @export
synthesizePositiveControls <- function(connectionDetails,
                                       cdmDatabaseSchema,
                                       oracleTempSchema = NULL,
                                       tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
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
                                       minModelCount = 5,
                                       covariateSettings = FeatureExtraction::createCovariateSettings(
                                         useDemographicsAgeGroup = TRUE,
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
                                         endDays = 0
                                       ),
                                       prior = Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                                       control = Cyclops::createControl(
                                         cvType = "auto",
                                         startingVariance = 0.1,
                                         seed = 1,
                                         resetCoefficients = TRUE,
                                         noiseLevel = "quiet",
                                         threads = 10
                                       ),
                                       firstExposureOnly = FALSE,
                                       washoutPeriod = 183,
                                       riskWindowStart = 0,
                                       riskWindowEnd = 0,
                                       endAnchor = "cohort end",
                                       minDaysAtRisk = 2,
                                       addIntentToTreat = FALSE,
                                       firstOutcomeOnly = FALSE,
                                       removePeopleWithPriorOutcomes = FALSE,
                                       maxSubjectsForModel = 1e+05,
                                       effectSizes = c(1, 1.25, 1.5, 2, 4),
                                       precision = 0.01,
                                       outputIdOffset = 1000,
                                       workFolder = "./SignalInjectionTemp",
                                       cdmVersion = NULL,
                                       modelThreads = 1,
                                       generationThreads = 1) {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warning("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.")
    tempEmulationSchema <- oracleTempSchema
  }
  if (!is.null(cdmVersion) && cdmVersion != "") {
    warning("The 'cdmVersion' argument is deprecated.")
  }
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
  checkmate::assertLogical(createOutputTable, len = 1, add = errorMessages)
  checkmate::assertDataFrame(exposureOutcomePairs, add = errorMessages)
  checkmate::assertNames(colnames(exposureOutcomePairs), must.include = c("exposureId", "outcomeId"), add = errorMessages)
  checkmate::assertChoice(modelType, c("poisson", "survival"), add = errorMessages)
  checkmate::assertInt(minOutcomeCountForModel, lower = 0, add = errorMessages)
  checkmate::assertInt(minOutcomeCountForInjection, lower = 0, add = errorMessages)
  checkmate::assertInt(minModelCount, lower = 0, add = errorMessages)
  checkmate::assertList(covariateSettings, add = errorMessages)
  checkmate::assertClass(prior, "cyclopsPrior", add = errorMessages)
  checkmate::assertClass(control, "cyclopsControl", add = errorMessages)
  checkmate::assertLogical(firstExposureOnly, len = 1, add = errorMessages)
  checkmate::assertInt(washoutPeriod, lower = 0, add = errorMessages)
  checkmate::assertInt(riskWindowStart, add = errorMessages)
  checkmate::assertInt(riskWindowEnd, add = errorMessages)
  checkmate::assertInt(minDaysAtRisk, lower = 1, add = errorMessages)
  checkmate::assertLogical(removePeopleWithPriorOutcomes, len = 1, add = errorMessages)
  checkmate::assertInt(maxSubjectsForModel, lower = 0, add = errorMessages)
  checkmate::assertNumeric(effectSizes, lower = 1, min.len = 1, add = errorMessages)
  checkmate::assertNumeric(precision, lower = 0, len = 1, add = errorMessages)
  checkmate::assertInt(outputIdOffset, lower = 0, add = errorMessages)
  checkmate::assertCharacter(workFolder, len = 1, add = errorMessages)
  checkmate::assertInt(modelThreads, lower = 1, add = errorMessages)
  checkmate::assertInt(generationThreads, lower = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  if (!firstExposureOnly && addIntentToTreat) {
    stop("Cannot have addIntentToTreat = TRUE and firstExposureOnly = FALSE at the same time")
  }
  if (modelType == "poisson" && addIntentToTreat) {
    stop("Intent to treat injection not yet implemented for Poisson models")
  }
  if (!grepl("start$|end$", endAnchor, ignore.case = TRUE)) {
    stop("endAnchor should have value 'cohort start' or 'cohort end'")
  }
  if (!file.exists(workFolder)) {
    dir.create(workFolder)
  }
  result <- loadDataFitModels(connectionDetails = connectionDetails,
                              cdmDatabaseSchema = cdmDatabaseSchema,
                              tempEmulationSchema = tempEmulationSchema,
                              exposureDatabaseSchema = exposureDatabaseSchema,
                              exposureTable = exposureTable,
                              outcomeDatabaseSchema = outcomeDatabaseSchema,
                              outcomeTable = outcomeTable,
                              exposureOutcomePairs = exposureOutcomePairs,
                              modelType = modelType,
                              minOutcomeCountForModel = minOutcomeCountForModel,
                              minModelCount = minModelCount,
                              covariateSettings = covariateSettings,
                              prior = prior,
                              control = control,
                              firstExposureOnly = firstExposureOnly,
                              washoutPeriod = washoutPeriod,
                              riskWindowStart = riskWindowStart,
                              riskWindowEnd = riskWindowEnd,
                              endAnchor = endAnchor,
                              minDaysAtRisk = minDaysAtRisk,
                              firstOutcomeOnly = firstOutcomeOnly,
                              removePeopleWithPriorOutcomes = removePeopleWithPriorOutcomes,
                              maxSubjectsForModel = maxSubjectsForModel,
                              effectSizes = effectSizes,
                              outputIdOffset = outputIdOffset,
                              workFolder = workFolder,
                              modelThreads = modelThreads)
  
  result <- generateAllOutcomes(workFolder = workFolder,
                                generationThreads = generationThreads,
                                minOutcomeCountForInjection = minOutcomeCountForInjection,
                                removePeopleWithPriorOutcomes = removePeopleWithPriorOutcomes,
                                modelType = modelType,
                                effectSizes = effectSizes,
                                precision = precision,
                                addIntentToTreat = addIntentToTreat,
                                result = result)
  summaryFile <- file.path(workFolder, "summary.rds")
  saveRDS(result, summaryFile)
  if (is.null(getOption("skipPositiveControlUpload")) || !getOption("skipPositiveControlUpload")) {
    insertOutcomes(connectionDetails = connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   outputDatabaseSchema = outputDatabaseSchema,
                   outputTable = outputTable,
                   outcomeDatabaseSchema = outcomeDatabaseSchema,
                   outcomeTable = outcomeTable,
                   tempEmulationSchema = tempEmulationSchema,
                   createOutputTable = createOutputTable,
                   result = result) 
  }
  return(result)
}

loadDataFitModels <- function(connectionDetails,
                              cdmDatabaseSchema,
                              tempEmulationSchema,
                              exposureDatabaseSchema,
                              exposureTable,
                              outcomeDatabaseSchema,
                              outcomeTable,
                              exposureOutcomePairs,
                              modelType,
                              minOutcomeCountForModel,
                              minModelCount,
                              covariateSettings,
                              prior,
                              control,
                              firstExposureOnly,
                              washoutPeriod,
                              riskWindowStart,
                              riskWindowEnd,
                              endAnchor,
                              minDaysAtRisk,
                              firstOutcomeOnly,
                              removePeopleWithPriorOutcomes,
                              maxSubjectsForModel,
                              effectSizes,
                              outputIdOffset,
                              workFolder,
                              modelThreads){
  # Find all exposures for each outcome, then identify unique groups of exposures
  group <- function(outcomeId) {
    exposureIds <- exposureOutcomePairs$exposureId[exposureOutcomePairs$outcomeId == outcomeId]
    return(exposureIds[order(exposureIds)])
  }
  outcomeIds <- unique(exposureOutcomePairs$outcomeId)
  groups <- purrr::map(unique(outcomeIds), group)
  uniqueGroups <- unique(groups)
  saveRDS(uniqueGroups, file.path(workFolder, "uniqueGroups.rds"))
  outcomeIdToGroupId <- data.frame(outcomeIds = outcomeIds, groupIds = match(groups, uniqueGroups))
  saveRDS(outcomeIdToGroupId, file.path(workFolder, "outcomeIdToGroupId.rds"))
  
  result <- tibble(
    exposureId = rep(exposureOutcomePairs$exposureId, each = length(effectSizes)),
    outcomeId = rep(exposureOutcomePairs$outcomeId, each = length(effectSizes)),
    targetEffectSize = rep(effectSizes, nrow(exposureOutcomePairs)),
    newOutcomeId = outputIdOffset + (0:(nrow(exposureOutcomePairs) * length(effectSizes) - 1)),
    trueEffectSize = 0,
    trueEffectSizeFirstExposure = 0,
    trueEffectSizeItt = 0,
    injectedOutcomes = 0,
    modelFolder = "",
    outcomesToInjectFile = ""
  )
  
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  result <- loadDataForSynthesis(connection = connection,
                                 cdmDatabaseSchema = cdmDatabaseSchema,
                                 tempEmulationSchema = tempEmulationSchema,
                                 exposureDatabaseSchema = exposureDatabaseSchema,
                                 exposureTable = exposureTable,
                                 outcomeDatabaseSchema = outcomeDatabaseSchema,
                                 outcomeTable = outcomeTable,
                                 exposureOutcomePairs = exposureOutcomePairs,
                                 modelType = modelType,
                                 firstExposureOnly = firstExposureOnly,
                                 washoutPeriod = washoutPeriod,
                                 riskWindowStart = riskWindowStart,
                                 riskWindowEnd = riskWindowEnd,
                                 endAnchor = endAnchor,
                                 minDaysAtRisk = minDaysAtRisk,
                                 firstOutcomeOnly = firstOutcomeOnly,
                                 removePeopleWithPriorOutcomes = removePeopleWithPriorOutcomes,
                                 workFolder = workFolder,
                                 result = result) 
  
  getCovariatesForModels(connection = connection,
                         cdmDatabaseSchema = cdmDatabaseSchema,
                         tempEmulationSchema = tempEmulationSchema,
                         covariateSettings = covariateSettings,
                         maxSubjectsForModel = maxSubjectsForModel,
                         workFolder = workFolder,
                         uniqueGroups = uniqueGroups)
  
  result <- buildModels(outcomeIdToGroupId = outcomeIdToGroupId,
                        uniqueGroups = uniqueGroups,
                        exposureOutcomePairs = exposureOutcomePairs,
                        effectSizes = effectSizes,
                        minModelCount = minModelCount,
                        minOutcomeCountForModel = minOutcomeCountForModel,
                        workFolder = workFolder,
                        modelThreads = modelThreads,
                        removePeopleWithPriorOutcomes = removePeopleWithPriorOutcomes,
                        modelType = modelType,
                        prior = prior,
                        control = control,
                        result = result) 
  
  getCovariatesOutsideSample(uniqueGroups = uniqueGroups,
                             outcomeIdToGroupId = outcomeIdToGroupId,
                             connection = connection,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             tempEmulationSchema = tempEmulationSchema,
                             covariateSettings = covariateSettings,
                             workFolder = workFolder,
                             result = result)
  
  sql <- "TRUNCATE TABLE #cohort_person; DROP TABLE #cohort_person;"
  sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection), tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  return(result)
}

loadDataForSynthesis <- function(connection,
                                 cdmDatabaseSchema,
                                 tempEmulationSchema,
                                 exposureDatabaseSchema,
                                 exposureTable,
                                 outcomeDatabaseSchema,
                                 outcomeTable,
                                 exposureOutcomePairs,
                                 modelType,
                                 firstExposureOnly,
                                 washoutPeriod,
                                 riskWindowStart,
                                 riskWindowEnd,
                                 endAnchor,
                                 minDaysAtRisk,
                                 firstOutcomeOnly,
                                 removePeopleWithPriorOutcomes,
                                 workFolder,
                                 result = result) {
  exposureIds <- unique(exposureOutcomePairs$exposureId)
  
  exposuresFile <- file.path(workFolder, "exposures.rds")
  outcomesFile <- file.path(workFolder, "outcomes.rds")
  priorOutcomesFile <- file.path(workFolder, "priorOutcomes.rds")
  countsFile <- file.path(workFolder, "counts.rds")
  
  # Create exposure cohorts ------------------------------------
  renderedSql <- SqlRender::loadRenderTranslateSql("CreateExposedCohorts.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = DatabaseConnector::dbms(connection),
                                                   tempEmulationSchema = tempEmulationSchema,
                                                   cdm_database_schema = cdmDatabaseSchema,
                                                   exposure_ids = exposureIds,
                                                   washout_period = washoutPeriod,
                                                   exposure_database_schema = exposureDatabaseSchema,
                                                   exposure_table = exposureTable,
                                                   first_exposure_only = firstExposureOnly,
                                                   risk_window_start = riskWindowStart,
                                                   risk_window_end = riskWindowEnd,
                                                   add_exposure_days_to_end = grepl("end$", endAnchor, ignore.case = TRUE),
                                                   min_days_at_risk = minDaysAtRisk
  )
  
  DatabaseConnector::executeSql(connection, renderedSql)
  
  # Get exposure cohorts from database -------------------------------
  if (file.exists(exposuresFile)) {
    exposures <- readRDS(exposuresFile)
  } else {
    ParallelLogger::logInfo("\nRetrieving exposure cohorts")
    exposureSql <- SqlRender::loadRenderTranslateSql("GetExposedCohorts.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = DatabaseConnector::dbms(connection),
                                                     tempEmulationSchema = tempEmulationSchema
    )
    exposures <- DatabaseConnector::querySql(connection, exposureSql, snakeCaseToCamelCase = TRUE)
    exposures <- exposures[order(exposures$rowId), ]
    saveRDS(exposures, exposuresFile)
  }
  
  # Get prior outcomes (if needed) ------------------------------------
  if (removePeopleWithPriorOutcomes) {
    if (file.exists(priorOutcomesFile)) {
      priorOutcomes <- readRDS(priorOutcomesFile)
    } else {
      ParallelLogger::logInfo("Finding people with prior outcomes")
      table <- exposureOutcomePairs
      colnames(table) <- SqlRender::camelCaseToSnakeCase(colnames(table))
      DatabaseConnector::insertTable(
        connection = connection,
        tableName = "#exposure_outcome",
        data = table,
        dropTableIfExists = TRUE,
        createTable = TRUE,
        tempTable = TRUE,
        tempEmulationSchema = tempEmulationSchema
      )
      outcomeSql <- SqlRender::loadRenderTranslateSql("GetPriorOutcomes.sql",
                                                      packageName = "MethodEvaluation",
                                                      dbms = DatabaseConnector::dbms(connection),
                                                      tempEmulationSchema = tempEmulationSchema,
                                                      cdm_database_schema = cdmDatabaseSchema,
                                                      outcome_database_schema = outcomeDatabaseSchema,
                                                      outcome_table = outcomeTable,
                                                      first_outcome_only = firstOutcomeOnly
      )
      priorOutcomes <- DatabaseConnector::querySql(connection, outcomeSql, snakeCaseToCamelCase = TRUE)
      saveRDS(priorOutcomes, priorOutcomesFile)
      sql <- "TRUNCATE TABLE #exposure_outcome; DROP TABLE #exposure_outcome;"
      sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection), tempEmulationSchema = tempEmulationSchema)
      DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
    }
  }
  
  # Get outcomes ---------------------------------------
  if (file.exists(outcomesFile)) {
    outcomeCounts <- readRDS(outcomesFile)
  } else {
    ParallelLogger::logInfo("Extracting outcome counts")
    table <- exposureOutcomePairs
    colnames(table) <- SqlRender::camelCaseToSnakeCase(colnames(table))
    DatabaseConnector::insertTable(
      connection = connection,
      tableName = "#exposure_outcome",
      data = table,
      dropTableIfExists = TRUE,
      createTable = TRUE,
      tempTable = TRUE,
      tempEmulationSchema = tempEmulationSchema
    )
    outcomeSql <- SqlRender::loadRenderTranslateSql("GetOutcomes.sql",
                                                    packageName = "MethodEvaluation",
                                                    dbms = DatabaseConnector::dbms(connection),
                                                    tempEmulationSchema = tempEmulationSchema,
                                                    cdm_database_schema = cdmDatabaseSchema,
                                                    outcome_database_schema = outcomeDatabaseSchema,
                                                    outcome_table = outcomeTable,
                                                    first_outcome_only = firstOutcomeOnly
    )
    outcomeCounts <- DatabaseConnector::querySql(connection, outcomeSql, snakeCaseToCamelCase = TRUE)
    saveRDS(outcomeCounts, outcomesFile)
    sql <- "TRUNCATE TABLE #exposure_outcome; DROP TABLE #exposure_outcome;"
    sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection), tempEmulationSchema = tempEmulationSchema)
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  
  # Generate counts for result object -------------------------------
  if (file.exists(countsFile)) {
    result <- readRDS(countsFile)
  } else {
    ParallelLogger::logInfo("Computing counts per exposure - outcome pair")
    temp <- select(exposures, .data$rowId, .data$exposureId, .data$eraNumber) %>%
      inner_join(select(outcomeCounts, .data$rowId, .data$outcomeId, .data$y, .data$yItt),
                 by = "rowId"
      )
    if (modelType == "survival") {
      temp <- temp %>%
        mutate(
          y = .data$y != 0,
          yItt = .data$yItt != 0
        )
    }
    generateCounts <- function(outcomeId) {
      tempOutcomes <- temp %>%
        filter(.data$outcomeId == !!outcomeId)
      if (removePeopleWithPriorOutcomes) {
        removeRowIds <- priorOutcomes %>%
          filter(.data$outcomeId == !!outcomeId) %>%
          pull(.data$rowId)
        tempOutcomes <- tempOutcomes %>%
          filter(!.data$rowId %in% removeRowIds)
        tempExposures <- exposures %>%
          filter(!.data$rowId %in% removeRowIds)
      } else {
        tempExposures <- exposures
      }
      exposureSummary <- tempExposures %>%
        group_by(.data$exposureId) %>%
        summarize(
          exposures = n(),
          firstExposures = sum(.data$eraNumber == 1),
          .groups = "drop_last"
        )
      outcomeSummary <- tempOutcomes %>%
        group_by(.data$exposureId) %>%
        summarize(
          observedOutcomes = sum(.data$y),
          observedOutcomesFirstExposure = sum(.data$y & .data$eraNumber == 1),
          observedOutcomesItt = sum(.data$yItt),
          observedOutcomesFirstExposureItt = sum(.data$yItt & .data$eraNumber == 1),
          .groups = "drop_last"
        )
      resultRows <- exposureSummary %>%
        left_join(outcomeSummary, by = "exposureId") %>%
        mutate(outcomeId = !!outcomeId)
      return(resultRows)
    }
    resultRows <- purrr::map_dfr(unique(result$outcomeId), generateCounts)
    result <- left_join(result, resultRows, by = c("exposureId", "outcomeId")) %>%
      mutate(
        exposures = case_when(
          is.na(exposures) ~ as.integer(0),
          TRUE ~ as.integer(.data$exposures)
        ),
        firstExposures = case_when(
          is.na(firstExposures) ~ as.integer(0),
          TRUE ~ as.integer(.data$firstExposures)
        ),
        observedOutcomes = case_when(
          is.na(observedOutcomes) ~ as.integer(0),
          TRUE ~ as.integer(.data$observedOutcomes)
        ),
        observedOutcomesFirstExposure = case_when(
          is.na(observedOutcomesFirstExposure) ~ as.integer(0),
          TRUE ~ as.integer(.data$observedOutcomesFirstExposure)
        ),
        observedOutcomesItt = case_when(
          is.na(observedOutcomesItt) ~ as.integer(0),
          TRUE ~ as.integer(.data$observedOutcomesItt)
        ),
        observedOutcomesFirstExposureItt = case_when(
          is.na(observedOutcomesFirstExposureItt) ~ as.integer(0),
          TRUE ~ as.integer(.data$observedOutcomesFirstExposureItt)
        )
      )
    saveRDS(result, countsFile)
  }
  return(result)
}

getCovariatesForModels <- function(connection,
                                   cdmDatabaseSchema,
                                   tempEmulationSchema,
                                   covariateSettings,
                                   maxSubjectsForModel,
                                   workFolder,
                                   uniqueGroups) {
  
  # Fetch covariates for each group of exposures
  for (i in 1:length(uniqueGroups)) {
    covarFileName <- file.path(workFolder, sprintf("covarsForModel_g%s.zip", i))
    if (!file.exists(covarFileName)) {
      cohortIds <- uniqueGroups[[i]]
      sql <- "SELECT COUNT(*) AS entries FROM (SELECT DISTINCT subject_id, cohort_start_date FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids)) tmp;"
      count <- DatabaseConnector::renderTranslateQuerySql(
        connection = connection,
        sql = sql,
        tempEmulationSchema = tempEmulationSchema,
        cohort_ids = cohortIds,
        snakeCaseToCamelCase = TRUE
      )$entries
      if (count > maxSubjectsForModel) {
        ParallelLogger::logInfo("Sampling exposed cohorts for model(s)")
        renderedSql <- SqlRender::loadRenderTranslateSql("SampleExposedCohorts.sql",
                                                         packageName = "MethodEvaluation",
                                                         dbms = DatabaseConnector::dbms(connection),
                                                         tempEmulationSchema = tempEmulationSchema,
                                                         sample_size = maxSubjectsForModel,
                                                         cohort_ids = cohortIds
        )
        DatabaseConnector::executeSql(connection, renderedSql)
        
        sql <- "SELECT row_id FROM #sampled_person"
        sampledRowIds <- DatabaseConnector::renderTranslateQuerySql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          snakeCaseToCamelCase = TRUE
        )
        sampledExposuresFile <- file.path(workFolder, sprintf("sampledRowIds_g%s.rds", i))
        saveRDS(sampledRowIds, sampledExposuresFile)
      } else {
        # Don't sample, just copy:
        sql <- "SELECT * INTO #sampled_person FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids);"
        DatabaseConnector::renderTranslateExecuteSql(
          connection = connection,
          sql = sql,
          tempEmulationSchema = tempEmulationSchema,
          cohort_ids = cohortIds,
          progressBar = FALSE,
          reportOverallTime = FALSE
        )
      }
      ParallelLogger::logInfo("Extracting covariates for fitting outcome model(s)")
      covariateData <- FeatureExtraction::getDbCovariateData(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        cdmDatabaseSchema = cdmDatabaseSchema,
        cohortTable = "#sampled_person",
        cohortTableIsTemp = TRUE,
        rowIdField = "row_id",
        covariateSettings = covariateSettings,
        aggregated = FALSE
      )
      covariateData <- FeatureExtraction::tidyCovariateData(
        covariateData = covariateData,
        normalize = TRUE,
        removeRedundancy = TRUE
      )
      FeatureExtraction::saveCovariateData(covariateData, covarFileName)
      
      sql <- "TRUNCATE TABLE #sampled_person; DROP TABLE #sampled_person;"
      DatabaseConnector::renderTranslateExecuteSql(
        connection = connection,
        tempEmulationSchema = tempEmulationSchema,
        sql = sql,
        progressBar = FALSE,
        reportOverallTime = FALSE
      )
    }
  }
}

buildModels <- function(outcomeIdToGroupId,
                        uniqueGroups,
                        exposureOutcomePairs,
                        effectSizes,
                        minModelCount,
                        minOutcomeCountForModel,
                        workFolder,
                        modelThreads,
                        removePeopleWithPriorOutcomes,
                        modelType,
                        prior,
                        control,
                        result) {
  ParallelLogger::logInfo("Fitting outcome models")
  exposuresFile <- file.path(workFolder, "exposures.rds")
  outcomesFile <- file.path(workFolder, "outcomes.rds")
  priorOutcomesFile <- file.path(workFolder, "priorOutcomes.rds")
  
  tasks <- list()
  modelsWithEnoughOutcomes <- 0
  outcomeIds <- unique(exposureOutcomePairs$outcomeId)
  for (i in 1:length(outcomeIds)) {
    outcomeId <- outcomeIds[i]
    groupId <- outcomeIdToGroupId$groupId[outcomeIdToGroupId$outcomeId == outcomeId]
    groupExposureIds <- uniqueGroups[[groupId]]
    idx <- result$outcomeId == outcomeId &
      result$exposureId %in% groupExposureIds &
      result$targetEffectSize == effectSizes[1]
    if (sum(result$observedOutcomes[idx]) >= minOutcomeCountForModel) {
      modelsWithEnoughOutcomes <- modelsWithEnoughOutcomes + 1
      modelFolder <- file.path(workFolder, paste0("model_o", outcomeId))
      result$modelFolder[result$outcomeId == outcomeId] <- modelFolder
      if (!file.exists(modelFolder)) {
        task <- list(
          outcomeId = outcomeId,
          modelFolder = modelFolder,
          covarFileName = file.path(workFolder, sprintf("covarsForModel_g%s.zip", groupId)),
          sampledExposuresFile = file.path(workFolder, sprintf("sampledRowIds_g%s.rds", groupId)),
          groupExposureIds = groupExposureIds
        )
        tasks[[length(tasks) + 1]] <- task
      }
    }
  }
  if (modelsWithEnoughOutcomes < minModelCount) {
    stop(
      "Not enough negative controls with sufficent outcome count to fit outcome model. Found ",
      modelsWithEnoughOutcomes,
      " controls with enough outcomes (> ",
      minOutcomeCountForModel,
      "), but require ",
      minModelCount,
      " controls."
    )
  }
  
  if (length(tasks) > 0) {
    cluster <- ParallelLogger::makeCluster(modelThreads)
    ParallelLogger::clusterApply(
      cluster,
      tasks,
      fitModel,
      exposuresFile,
      outcomesFile,
      priorOutcomesFile,
      removePeopleWithPriorOutcomes,
      modelType,
      prior,
      control
    )
    ParallelLogger::stopCluster(cluster)
  }
  return(result)
}

fitModel <- function(task,
                     exposuresFile,
                     outcomesFile,
                     priorOutcomesFile,
                     removePeopleWithPriorOutcomes,
                     modelType,
                     prior,
                     control) {
  ParallelLogger::logInfo("Fitting model for outcome ", task$outcomeId)
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
  # Dedupe exposures for model fitting, so we don't overfit:
  exposures <- exposures[order(exposures$personId, exposures$cohortStartDate), ]
  exposures <- exposures[!duplicated(exposures[, c("personId", "cohortStartDate")]), ]
  
  covariateData <- FeatureExtraction::loadCovariateData(task$covarFileName)
  covariates <- covariateData$covariates %>%
    filter(.data$rowId %in% local(exposures$rowId))
  
  if (removePeopleWithPriorOutcomes) {
    priorOutcomes <- readRDS(priorOutcomesFile)
    removeRowIds <- priorOutcomes$rowId[priorOutcomes$outcomeId == task$outcomeId]
    outcomes <- outcomes[!(outcomes$rowId %in% removeRowIds), ]
    exposures <- exposures[!(exposures$rowId %in% removeRowIds), ]
    covariates <- covariates %>%
      filter(!.data$rowId %in% removeRowIds)
  }
  outcomes <- merge(exposures, outcomes[, c(
    "rowId",
    "y",
    "timeToEvent"
  )], by = c("rowId"), all.x = TRUE)
  outcomes <- outcomes[order(outcomes$rowId), ]
  outcomes$y[is.na(outcomes$y)] <- 0
  names(outcomes)[names(outcomes) == "daysAtRisk"] <- "time"
  if (modelType == "survival") {
    # For survival, time is either the time to the end of the risk window, or the event
    outcomes$y[outcomes$y != 0] <- 1
    outcomes$time[outcomes$y != 0] <- outcomes$timeToEvent[outcomes$y != 0]
  }
  outcomes$time <- outcomes$time + 1
  
  # Note: for survival, using Poisson regression with 1 outcome and censored time as equivalent of
  # survival regression:
  covariateData$outcomes <- outcomes
  cyclopsData <- Cyclops::convertToCyclopsData(covariateData$outcomes,
                                               covariates,
                                               modelType = "pr",
                                               quiet = TRUE
  )
  fit <- tryCatch(
    {
      Cyclops::fitCyclopsModel(cyclopsData, prior = prior, control = control)
    },
    error = function(e) {
      e$message
    }
  )
  if (fit$return_flag != "SUCCESS") {
    fit <- fit$return_flag
  }
  if (is.character(fit)) {
    ParallelLogger::logInfo(paste("Unable to fit model for outcome", task$outcomeId, ":", fit))
    dir.create(task$modelFolder)
    write.csv(fit, file.path(task$modelFolder, "Error.txt"))
  } else {
    betas <- coef(fit)
    intercept <- tibble(
      beta = betas[1],
      id = bit64::as.integer64(0),
      covariateName = "(Intercept)",
      row.names = NULL
    )
    betas <- betas[betas != 0]
    if (length(betas) > 1) {
      betas <- betas[2:length(betas)]
      betas <- tibble(beta = betas, covariateId = bit64::as.integer64(attr(betas, "names")))
      betas <- betas %>%
        inner_join(
          covariateData$covariateRef %>%
            collect() %>%
            mutate(covariateId = bit64::as.integer64(.data$covariateId)),
          by = "covariateId"
        ) %>%
        select(.data$beta, id = .data$covariateId, .data$covariateName) %>%
        arrange(desc(abs(.data$beta)))
      betas <- bind_rows(intercept, betas)
    } else {
      betas <- intercept
    }
    dir.create(task$modelFolder)
    saveRDS(betas, file.path(task$modelFolder, "betas.rds"))
  }
}

getCovariatesOutsideSample <- function(uniqueGroups,
                                       outcomeIdToGroupId,
                                       connection,
                                       cdmDatabaseSchema,
                                       tempEmulationSchema,
                                       covariateSettings,
                                       workFolder,
                                       result) {
  # Fetch covariates for all rows if covars for model were based on a sample 
  for (i in 1:length(uniqueGroups)) {
    sampledExposuresFile <- file.path(workFolder, sprintf("sampledRowIds_g%s.rds", i))
    covarFileName <- file.path(workFolder, sprintf("covarsForPrediction_g%s.zip", i))
    if (file.exists(sampledExposuresFile) && !file.exists(covarFileName)) {
      ParallelLogger::logInfo("Extracting covariates for all rows predicting outcomes")
      if (!is(covariateSettings, "covariateSettings")) {
        stop("Composite covariate settings not supported")
      }
      outcomeIds <- outcomeIdToGroupId$outcomeId[outcomeIdToGroupId$groupId == i]
      modelCovariateIds <- c()
      for (modelFolder in unique(result$modelFolder[result$outcomeId %in% outcomeIds])) {
        if (modelFolder != "" && file.exists(file.path(modelFolder, "betas.rds"))) {
          betas <- readRDS(file.path(modelFolder, "betas.rds"))
          if (is.null(modelCovariateIds)) {
            modelCovariateIds <- betas$id
          } else {
            modelCovariateIds <- c(modelCovariateIds, betas$id)
          }
        }
      }
      modelCovariateIds <- unique(modelCovariateIds)
      modelCovariateIds <- modelCovariateIds[modelCovariateIds != 0]
      
      ParallelLogger::logInfo(paste(
        "Number of unique covariates across outcome models:",
        length(modelCovariateIds)
      ))
      if (length(modelCovariateIds) == 0) {
        dir.create(covarFileName)
      } else {
        covariateSettings$includedCovariateIds <- modelCovariateIds
        sql <- "SELECT * INTO #selected_person FROM #cohort_person WHERE cohort_definition_id IN (@cohort_ids);"
        sql <- SqlRender::render(sql, cohort_ids = uniqueGroups[[i]])
        sql <- SqlRender::translate(sql,
                                    targetDialect = DatabaseConnector::dbms(connection),
                                    tempEmulationSchema = tempEmulationSchema
        )
        DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
        covariateData <- FeatureExtraction::getDbCovariateData(
          connection = connection,
          tempEmulationSchema = tempEmulationSchema,
          cdmDatabaseSchema = cdmDatabaseSchema,
          cohortTable = "#selected_person",
          cohortTableIsTemp = TRUE,
          rowIdField = "row_id",
          covariateSettings = covariateSettings,
          aggregated = FALSE
        )
        covariateData <- FeatureExtraction::tidyCovariateData(
          covariateData = covariateData,
          normalize = TRUE,
          removeRedundancy = FALSE
        )
        FeatureExtraction::saveCovariateData(covariateData, covarFileName)
        
        sql <- "TRUNCATE TABLE #selected_person; DROP TABLE #selected_person;"
        sql <- SqlRender::translate(sql,
                                    targetDialect = DatabaseConnector::dbms(connection),
                                    tempEmulationSchema = tempEmulationSchema
        )
        DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
      }
    }
  }
}


generateAllOutcomes <- function(workFolder,
                                generationThreads,
                                minOutcomeCountForInjection,
                                removePeopleWithPriorOutcomes,
                                modelType,
                                effectSizes,
                                precision,
                                addIntentToTreat,
                                result) {
  exposuresFile <- file.path(workFolder, "exposures.rds")
  outcomesFile <- file.path(workFolder, "outcomes.rds")
  priorOutcomesFile <- file.path(workFolder, "priorOutcomes.rds")
  outcomeIdToGroupId <- readRDS(file.path(workFolder, "outcomeIdToGroupId.rds"))
  
  ParallelLogger::logInfo("Generating outcomes")
  temp <- result[result$observedOutcomes > minOutcomeCountForInjection, c(
    "exposureId",
    "outcomeId",
    "modelFolder"
  )]
  temp <- temp[temp$modelFolder != "", ]
  temp <- unique(temp)
  createTask <- function(i) {
    groupId <- outcomeIdToGroupId$groupId[outcomeIdToGroupId$outcomeId == temp$outcomeId[i]]
    sampledExposuresFile <- file.path(workFolder, sprintf("sampledRowIds_g%s.rds", groupId))
    if (file.exists(sampledExposuresFile)) {
      covarFileName <- file.path(workFolder, sprintf("covarsForPrediction_g%s.zip", groupId))
    } else {
      covarFileName <- file.path(workFolder, sprintf("covarsForModel_g%s.zip", groupId))
    }
    task <- list(
      exposureId = temp$exposureId[i],
      outcomeId = temp$outcomeId[i],
      modelFolder = temp$modelFolder[i],
      covarFileName = covarFileName
    )
    return(task)
  }
  if (nrow(temp) > 0) {
    tasks <- lapply(1:nrow(temp), createTask)
    cluster <- ParallelLogger::makeCluster(generationThreads)
    results <- ParallelLogger::clusterApply(
      cluster,
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
      workFolder,
      addIntentToTreat
    )
    ParallelLogger::stopCluster(cluster)
    result <- do.call("rbind", results)
  }
  return(result)
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
                             workFolder,
                             addIntentToTreat) {
  ParallelLogger::logInfo(
    "Generating outcomes for exposure ",
    task$exposureId,
    " and outcome ",
    task$outcomeId
  )
  set.seed(1)
  resultSubset <- result |>
    filter(.data$exposureId == task$exposureId, .data$outcomeId == task$outcomeId)
  if (file.exists(file.path(task$modelFolder, "Error.txt"))) {
    return(resultSubset)
  } else {
    exposures <- readRDS(exposuresFile)
    exposures <- exposures |>
      filter(.data$exposureId == task$exposureId)
    predictionFile <- file.path(task$modelFolder, paste0("prediction_e", task$exposureId, ".rds"))
    if (file.exists(predictionFile)) {
      prediction <- readRDS(predictionFile)
    } else {
      betas <- readRDS(file.path(task$modelFolder, "betas.rds"))
      if (nrow(betas) == 1) {
        covariates <- NULL
      } else {
        covariateData <- FeatureExtraction::loadCovariateData(task$covarFileName)
        covariates <- covariateData$covariates %>%
          filter(.data$rowId %in% local(exposures$rowId))
      }
      prediction <- .predict(betas, exposures, covariates, modelType)
      saveRDS(prediction, predictionFile)
    }
    outcomes <- readRDS(outcomesFile)
    outcomes <- outcomes |>
      filter(.data$outcomeId == task$outcomeId)
    if (removePeopleWithPriorOutcomes) {
      priorOutcomes <- readRDS(priorOutcomesFile)
      removeRowIds <- priorOutcomes |>
        filter(.data$outcomeId == task$outcomeId)
      exposures <- exposures |>
        filter(!.data$rowId %in% removeRowIds)
      outcomes <- outcomes |>
        filter(!.data$rowId %in% removeRowIds)
    }
    exposures <- exposures |>
      inner_join(prediction, by = join_by("rowId"))
    exposures <- exposures |>
      left_join(outcomes, by = join_by("rowId"))
    exposures <- exposures |>
      mutate(y = if_else(is.na(.data$y), 0, .data$y),
             yItt = if_else(is.na(.data$yItt), 0, .data$yItt))
    for (fxSizeIdx in 1:length(effectSizes)) {
      effectSize <- effectSizes[fxSizeIdx]
      if (effectSize == 1) {
        newOutcomes <- data.frame()
        attr(newOutcomes, "injectedRr") <- 1
        attr(newOutcomes, "injectedRrFirstExposure") <- 1
        attr(newOutcomes, "injectedRrItt") <- 1
        outcomesToInjectFile <- ""
      } else {
        outcomesToInjectFile <- file.path(workFolder, paste0(
          "newOutcomes_e",
          task$exposureId,
          "_o",
          task$outcomeId,
          "_rr",
          effectSize,
          ".rds"
        ))
        if (file.exists(outcomesToInjectFile)) {
          newOutcomes <- readRDS(outcomesToInjectFile)
        } else {
          if (modelType == "poisson") {
            newOutcomes <- injectPoisson(exposures, effectSize, precision)
          } else {
            newOutcomes <- injectSurvival(exposures, effectSize, precision, addIntentToTreat)
          }
          newOutcomeId <- resultSubset$newOutcomeId[resultSubset$targetEffectSize == effectSize]
          if (nrow(newOutcomes) != 0) {
            newOutcomes$cohortStartDate <- as.Date(newOutcomes$cohortStartDate) + newOutcomes$timeToEvent
            newOutcomes$timeToEvent <- NULL
            newOutcomes$cohortDefinitionId <- newOutcomeId
            names(newOutcomes)[names(newOutcomes) == "personId"] <- "subjectId"
            saveRDS(newOutcomes, outcomesToInjectFile)
          }
        }
        ParallelLogger::logInfo(paste(
          "Target RR =",
          effectSize,
          ", injected RR =",
          attr(newOutcomes, "injectedRr"),
          ", injected RR during first exposure only =",
          attr(newOutcomes, "injectedRrFirstExposure") ,
          ", injected RR during ITT window =",
          attr(newOutcomes, "injectedRrItt") 
        ))
      }
      idx <- resultSubset$targetEffectSize == effectSize
      resultSubset$outcomesToInjectFile[idx] <- outcomesToInjectFile
      resultSubset$trueEffectSize[idx] <- attr(newOutcomes, "injectedRr")
      resultSubset$trueEffectSizeFirstExposure[idx] <- attr(newOutcomes, "injectedRrFirstExposure")
      resultSubset$trueEffectSizeItt[idx] <- attr(newOutcomes, "injectedRrItt")
      resultSubset$injectedOutcomes[idx] <- nrow(newOutcomes)
    }
    return(resultSubset)
  }
}

injectPoisson <- function(exposures, effectSize, precision, addIntentToTreat) {
  targetCount <- resultSubset$observedOutcomes[1] * (effectSize - 1)
  time <- exposures$daysAtRisk + 1
  newOutcomeCounts <- 0
  multiplier <- 1
  ratios <- c()
  while (round(abs(sum(newOutcomeCounts) - targetCount)) > precision * targetCount) {
    newOutcomeCounts <- rpois(
      nrow(exposures),
      multiplier * exposures$prediction * (effectSize - 1)
    )
    newOutcomeCounts[newOutcomeCounts > time] <- time[newOutcomeCounts > time]
    ratios <- c(ratios, sum(newOutcomeCounts) / targetCount)
    if (length(ratios) == 100) {
      multiplier <- multiplier * 1 / mean(ratios)
      ratios <- c()
      ParallelLogger::logDebug(paste(
        "Unable to achieve target RR using model as is. Adding multiplier of",
        multiplier,
        "to force target"
      ))
    }
  }
  idx <- which(newOutcomeCounts != 0)
  temp <- data.frame(
    personId = exposures$personId[idx],
    cohortStartDate = exposures$cohortStartDate[idx],
    time = exposures$daysAtRisk[idx] + 1,
    nOutcomes = newOutcomeCounts[idx]
  )
  outcomeRows <- sum(temp$nOutcomes)
  newOutcomes <- data.frame(
    personId = rep("", outcomeRows),
    cohortStartDate = rep(as.Date("1900-01-01"), outcomeRows),
    timeToEvent = rep(0, outcomeRows)
  )
  cursor <- 1
  for (i in 1:nrow(temp)) {
    nOutcomes <- temp$nOutcomes[i]
    if (nOutcomes != 0) {
      newOutcomes$personId[cursor:(cursor + nOutcomes - 1)] <- temp$personId[i]
      newOutcomes$cohortStartDate[cursor:(cursor + nOutcomes - 1)] <- temp$cohortStartDate[i]
      newOutcomes$timeToEvent[cursor:(cursor + nOutcomes - 1)] <- sample.int(
        size = nOutcomes,
        temp$time[i]
      ) - 1
      cursor <- cursor + nOutcomes
    }
  }
  injectedRr <- 1 + (nrow(newOutcomes) / resultSubset$observedOutcomes[1])
  
  # Count outcomes during first episodes:
  newOutcomeCountsFirstExposure <- sum(newOutcomeCounts[exposures$eraNumber == 1])
  injectedRrFirstExposure <- 1 +
    (newOutcomeCountsFirstExposure / resultSubset$observedOutcomesFirstExposure[1])
  
  attr(newOutcomes, "injectedRr") <- injectedRr
  attr(newOutcomes, "injectedRrFirstExposure") <- injectedRrFirstExposure
  attr(newOutcomes, "injectedRrItt") <- NA
  return(newOutcomes)
}

injectSurvival <- function(exposures, effectSize, precision, addIntentToTreat) {
  hasOutcome <- exposures$y != 0
  observedCount <- sum(hasOutcome)
  correctedTargetCount <- observedCount * (effectSize - 1)
  survivalTime <- as.numeric(exposures$daysAtRisk)
  survivalTime[hasOutcome] <- exposures$timeToEvent[hasOutcome]
  ratios <- c()
  multiplier <- 1
  hasNewOutcome <- c()
  ParallelLogger::logTrace("Generating outcomes during time-at-risk")
  while (round(abs(sum(hasNewOutcome) - correctedTargetCount)) > precision * correctedTargetCount) {
    timeToNewOutcome <- round(rexp(
      nrow(exposures),
      multiplier * exposures$prediction * (effectSize - 1)
    ))
    hasNewOutcome <- timeToNewOutcome < survivalTime
    # Correct for censored time and outcomes:
    observedRate <- (observedCount / sum(survivalTime))
    timeCensoringAtNewEvents <- (sum(survivalTime[!hasNewOutcome]) + sum(timeToNewOutcome[hasNewOutcome] + 1))
    requiredTotalEvents <- observedRate * timeCensoringAtNewEvents * effectSize
    correctedTargetCount <- requiredTotalEvents - sum(hasOutcome[!hasNewOutcome])
    if (correctedTargetCount < 0) {
      warning("Correct target count became negative. ")
    } else {
      ratios <- c(ratios, sum(hasNewOutcome) / correctedTargetCount)
    }
    if (length(ratios) == 100) {
      ParallelLogger::logDebug(paste(
        "Unable to achieve target RR using model as is. Correcting multiplier by",
        1 / mean(ratios),
        "to force target"
      ))
      multiplier <- mean(c(multiplier, multiplier * 1 / mean(ratios)))
      ratios <- c()
    }
    if (is.na(round(abs(sum(hasNewOutcome) - correctedTargetCount)))) {
      warning("NAs produced when generating outcomes")
    }
  }
  rateBefore <- observedCount / sum(survivalTime)
  rateAfter <- (sum(hasOutcome[!hasNewOutcome]) + sum(hasNewOutcome)) / (sum(survivalTime[!hasNewOutcome]) + sum(timeToNewOutcome[hasNewOutcome] + 1))
  injectedRr <- rateAfter / rateBefore
  newOutcomes <- data.frame(
    personId = exposures$personId[hasNewOutcome],
    cohortStartDate = exposures$cohortStartDate[hasNewOutcome],
    timeToEvent = timeToNewOutcome[hasNewOutcome]
  )
  # Count outcomes during first episodes:
  if (max(exposures$eraNumber) == 1) {
    injectedRrFirstExposure <- injectedRr
  } else {
    firstHasNewOutcome <- hasNewOutcome[exposures$eraNumber == 1]
    firstTimeToEvent <- timeToNewOutcome[exposures$eraNumber == 1]
    firstHasOutcome <- hasOutcome[exposures$eraNumber == 1]
    firstTime <- survivalTime[exposures$eraNumber == 1]
    firstObservedCount <- sum(firstHasOutcome)
    rateBeforeFirstExposure <- firstObservedCount / sum(firstTime)
    rateAfterFirstExposure <- (sum(firstHasOutcome[!firstHasNewOutcome]) + sum(firstHasNewOutcome)) / (sum(firstTime[!firstHasNewOutcome]) + sum(firstTimeToEvent[firstHasNewOutcome] + 1))
    injectedRrFirstExposure <- rateAfterFirstExposure / rateBeforeFirstExposure
  }
  
  if (addIntentToTreat) {
    hasOutcomeItt <- exposures$yItt > 0
    observedCountItt <- sum(hasOutcomeItt)
    correctedTargetCount <- observedCountItt * (effectSize - 1)
    survivalTimeItt <- exposures$daysObserved
    survivalTimeItt[hasOutcomeItt] <- exposures$timeToEvent[hasOutcomeItt]
    survivalTimeIttNew <- survivalTimeItt
    survivalTimeIttNew[hasNewOutcome] <- timeToNewOutcome[hasNewOutcome]
    rateBeforeItt <- observedCountItt / sum(survivalTimeItt)
    ratios <- c()
    multiplier <- rateBeforeItt / rateBefore # Adjust for fact that background rate may differ in ITT
    hasNewOutcomeItt <- c()
    ParallelLogger::logTrace("Generating outcomes for intent-to-treat")
    while (round(abs(sum(hasNewOutcomeItt) + sum(hasNewOutcome) - correctedTargetCount)) > precision *
           correctedTargetCount) {
      timeToNewOutcomeItt <- round(rexp(nrow(exposures), multiplier * exposures$prediction * (effectSize - 1)))
      hasNewOutcomeItt <- timeToNewOutcomeItt > exposures$daysAtRisk & timeToNewOutcomeItt < survivalTimeIttNew
      # Correct for censored time and outcomes:
      correctedTargetCount <- (observedCountItt / sum(survivalTimeItt)) *
        (sum(survivalTimeIttNew[!hasNewOutcomeItt]) + sum(timeToNewOutcomeItt[hasNewOutcomeItt] + 1)) *
        effectSize - sum(hasOutcomeItt[!hasNewOutcomeItt])
      if (correctedTargetCount < 0) {
        warning("Correct target count became negative. ")
      } else {
        ratios <- c(ratios, (sum(hasNewOutcomeItt) + sum(hasNewOutcome)) / correctedTargetCount)
      }
      if (length(ratios) == 100) {
        ParallelLogger::logDebug(paste(
          "Unable to achieve target RR using model as is. Correcting multiplier by",
          1 / mean(ratios),
          "to force target"
        ))
        multiplier <- mean(c(multiplier, multiplier * 1 / mean(ratios)))
        ratios <- c()
      }
      if (is.na(round(abs(sum(hasNewOutcome) - correctedTargetCount)))) {
        ParallelLogger::logDebug("Problem")
      }
    }
    rateAfterItt <- (sum(hasOutcomeItt[!hasNewOutcome & !hasNewOutcomeItt]) + sum(hasNewOutcome) + sum(hasNewOutcomeItt)) /
      (sum(survivalTimeIttNew[!hasNewOutcomeItt]) + sum(timeToNewOutcomeItt[hasNewOutcomeItt] + 1))
    injectedRrItt <- rateAfterItt / rateBeforeItt
    newOutcomes <- rbind(newOutcomes, data.frame(
      personId = exposures$personId[hasNewOutcomeItt],
      cohortStartDate = exposures$cohortStartDate[hasNewOutcomeItt],
      timeToEvent = timeToNewOutcomeItt[hasNewOutcomeItt]
    ))
  } else {
    injectedRrItt <- NA
  }
  attr(newOutcomes, "injectedRr") <- injectedRr
  attr(newOutcomes, "injectedRrFirstExposure") <- injectedRrFirstExposure
  attr(newOutcomes, "injectedRrItt") <- injectedRrItt
  return(newOutcomes)
}


insertOutcomes <- function(connectionDetails,
                           cdmDatabaseSchema,
                           outputDatabaseSchema,
                           outputTable,
                           outcomeDatabaseSchema,
                           outcomeTable,
                           tempEmulationSchema,
                           createOutputTable,
                           result) {
  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))
  
  # Insert outcomes into database ------------------------------------------------
  ParallelLogger::logInfo("Inserting additional outcomes into database")
  fileNames <- result$outcomesToInjectFile[result$outcomesToInjectFile != ""]
  outcomesToInject <- lapply(fileNames, readRDS)
  outcomesToInject <- bind_rows(outcomesToInject)
  if (Sys.getenv("USE_MPP_BULK_LOAD") == "TRUE" ||
      Sys.getenv("DATABASE_CONNECTOR_BULK_UPLOAD") == "TRUE") {
    tableName <- paste0(
      outputDatabaseSchema,
      ".temp_outcomes_",
      paste(sample(letters, 5), collapse = "")
    )
    tempTable <- FALSE
  } else {
    tableName <- "#temp_outcomes"
    tempTable <- TRUE
  }
  outcomesToInject$subjectId <- bit64::as.integer64(outcomesToInject$subjectId)
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = tableName,
    data = outcomesToInject,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = tempTable,
    tempEmulationSchema = tempEmulationSchema,
    progressBar = TRUE,
    camelCaseToSnakeCase = TRUE
  )
  
  toCopy <- result[result$modelFolder != "", c("outcomeId", "newOutcomeId")]
  DatabaseConnector::insertTable(
    connection = connection,
    tableName = "#to_copy",
    data = toCopy,
    dropTableIfExists = TRUE,
    createTable = TRUE,
    tempTable = TRUE,
    tempEmulationSchema = tempEmulationSchema,
    camelCaseToSnakeCase = TRUE
  )
  
  ParallelLogger::logInfo("Copying negative control outcomes into database")
  copySql <- SqlRender::loadRenderTranslateSql("CopyOutcomes.sql",
                                               packageName = "MethodEvaluation",
                                               dbms = DatabaseConnector::dbms(connection),
                                               cdm_database_schema = cdmDatabaseSchema,
                                               tempEmulationSchema = tempEmulationSchema,
                                               outcome_database_schema = outcomeDatabaseSchema,
                                               outcome_table = outcomeTable,
                                               output_database_schema = outputDatabaseSchema,
                                               output_table = outputTable,
                                               create_output_table = createOutputTable,
                                               temp_outcomes_table = tableName
  )
  
  DatabaseConnector::executeSql(connection, copySql)
  
  sql <- "TRUNCATE TABLE #to_copy; DROP TABLE #to_copy;"
  sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection), tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  sql <- "TRUNCATE TABLE @temp_outcomes_table; DROP TABLE @temp_outcomes_table;"
  sql <- SqlRender::render(sql, temp_outcomes_table = tableName)
  sql <- SqlRender::translate(sql, targetDialect = DatabaseConnector::dbms(connection), tempEmulationSchema = tempEmulationSchema)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
}

.predict <- function(betas, exposures, covariates, modelType) {
  intercept <- betas$beta[1]
  if (nrow(betas) == 1) {
    betas <- data.frame()
  } else {
    betas <- betas[2:nrow(betas), ]
    colnames(betas)[colnames(betas) == "id"] <- "covariateId"
  }
  if (nrow(betas) == 0) {
    prediction <- data.frame(
      rowId = exposures$rowId,
      daysAtRisk = exposures$daysAtRisk,
      value = exp(intercept)
    )
  } else {
    covariateIdIsInteger64 <- covariates %>%
      head(10000) %>%
      pull(.data$covariateId) %>%
      is("integer64")
    
    if (!covariateIdIsInteger64) {
      betas$covariateId <- as.numeric(betas$covariateId)
    }
    prediction <- covariates %>%
      inner_join(select(betas, .data$beta, .data$covariateId), by = "covariateId", copy = TRUE) %>%
      mutate(value = .data$beta * .data$covariateValue) %>%
      group_by(.data$rowId) %>%
      summarise(value = sum(.data$value, na.rm = TRUE)) %>%
      select(.data$rowId, .data$value) %>%
      ungroup() %>%
      collect()
    
    prediction <- prediction %>%
      right_join(select(exposures, .data$rowId, .data$daysAtRisk), by = "rowId") %>%
      mutate(value = coalesce(.data$value, 0) + intercept) %>%
      mutate(value = exp(.data$value))
  }
  if (modelType == "poisson") {
    prediction$value <- prediction$value * (prediction$daysAtRisk + 1)
  }
  prediction$daysAtRisk <- NULL
  colnames(prediction)[colnames(prediction) == "value"] <- "prediction"
  return(prediction)
}
