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

#' Create cohorts used in a reference set.
#'
#' @details
#' This function will create the outcomes of interest and nesting cohorts referenced in the various
#' reference sets. The outcomes of interest are derives using information like diagnoses, procedures,
#' and drug prescriptions. The outcomes are stored in a table on the database server.
#'
#' For the 'ohdsiMethodsBenchmark' reference set, the exposures are taken from the drug_era table, and
#' are therefore not generated as separate cohorts, and an exposure cohort table therefore needn't be supplied.
#' For the 'ohdsiDevelopment' reference set, exposure cohorts will be generated.
#'
#' @param connectionDetails       An R object of type \code{ConnectionDetails} created using the
#'                                function \code{createConnectionDetails} in the
#'                                \code{DatabaseConnector} package.
#' @param oracleTempSchema    DEPRECATED: use `tempEmulationSchema` instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @param cdmDatabaseSchema       A database schema containing health care data in the OMOP Commond
#'                                Data Model. Note that for SQL Server, botth the database and schema
#'                                should be specified, e.g. 'cdm_schema.dbo'.
#' @param exposureDatabaseSchema  The name of the database schema where the exposure cohorts will be
#'                                created. Only needed if \code{referenceSet = 'ohdsiDevelopment'}. Note
#'                                that for SQL Server, both the database and schema should be specified,
#'                                e.g. 'cdm_schema.dbo'.
#' @param exposureTable           The name of the table that will be created to store the exposure
#'                                cohorts. Only needed if \code{referenceSet = 'ohdsiDevelopment'}.
#' @param outcomeDatabaseSchema   The database schema where the target outcome table is located. Note
#'                                that for SQL Server, both the database and schema should be
#'                                specified, e.g. 'cdm_schema.dbo'
#' @param outcomeTable            The name of the table where the outcomes will be stored.
#' @param nestingDatabaseSchema   (For the OHDSI Methods Benchmark and OHDSI Development Set only) The
#'                                database schema where the nesting outcome table is located. Note that
#'                                for SQL Server, both the database and schema should be specified, e.g.
#'                                 'cdm_schema.dbo'.
#' @param nestingTable            (For the OHDSI Methods Benchmark and OHDSI Development Set only) The
#'                                name of the table where the nesting cohorts will be stored.
#' @param referenceSet            The name of the reference set for which outcomes need to be created.
#'                                Currently supported are "omopReferenceSet", "euadrReferenceSet",
#'                                "ohdsiMethodsBenchmark", and "ohdsiDevelopment".
#' @param workFolder              Name of local folder to place intermediary results; make sure to use
#'                                forward slashes (/). Do not use a folder on a network drive since
#'                                this greatly impacts performance.

#'
#' @export
createReferenceSetCohorts <- function(connectionDetails,
                                      oracleTempSchema = NULL,
                                      tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                      cdmDatabaseSchema,
                                      exposureDatabaseSchema = cdmDatabaseSchema,
                                      exposureTable = "exposures",
                                      outcomeDatabaseSchema = cdmDatabaseSchema,
                                      outcomeTable = "outcomes",
                                      nestingDatabaseSchema = cdmDatabaseSchema,
                                      nestingTable = "nesting",
                                      referenceSet = "ohdsiMethodsBenchmark",
                                      workFolder) {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warning("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.")
    tempEmulationSchema <- oracleTempSchema
  }
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(nestingDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(nestingTable, len = 1, add = errorMessages)
  checkmate::assertChoice(referenceSet, c("omopReferenceSet", "euadrReferenceSet", "ohdsiMethodsBenchmark", "ohdsiDevelopment"), add = errorMessages)
  checkmate::assertCharacter(workFolder, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)

  if (referenceSet == "omopReferenceSet") {
    ParallelLogger::logInfo("Generating HOIs for the OMOP reference set")
    renderedSql <- SqlRender::loadRenderTranslateSql("CreateOmopHois.sql",
      packageName = "MethodEvaluation",
      dbms = connectionDetails$dbms,
      cdm_database_schema = cdmDatabaseSchema,
      outcome_database_schema = outcomeDatabaseSchema,
      outcome_table = outcomeTable
    )
    conn <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(conn, renderedSql)
    ParallelLogger::logInfo("Done")
    DatabaseConnector::disconnect(conn)
  } else if (referenceSet == "euadrReferenceSet") {
    ParallelLogger::logInfo("Generating HOIs for the EU-ADR reference set")
    # TODO: add code for creating the EU-ADR HOIs
  } else if (referenceSet == "ohdsiMethodsBenchmark") {
    ParallelLogger::logInfo("Generating HOIs and nesting cohorts for the OHDSI Methods Benchmark")
    if (outcomeDatabaseSchema == nestingDatabaseSchema && outcomeTable == nestingTable) {
      stop("Outcome and nesting cohorts cannot be created in the same table")
    }
    createOhdsiNegativeControlCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      outcomeDatabaseSchema = outcomeDatabaseSchema,
      outcomeTable = outcomeTable,
      nestingDatabaseSchema = nestingDatabaseSchema,
      nestingTable = nestingTable,
      tempEmulationSchema = tempEmulationSchema,
      workFolder = workFolder
    )
  } else if (referenceSet == "ohdsiDevelopment") {
    ParallelLogger::logInfo("Generating HOIs and nesting cohorts for the OHDSI Development set")
    createOhdsiDevelopmentNegativeControlCohorts(
      connectionDetails = connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      exposureDatabaseSchema = exposureDatabaseSchema,
      exposureTable = exposureTable,
      outcomeDatabaseSchema = outcomeDatabaseSchema,
      outcomeTable = outcomeTable,
      nestingDatabaseSchema = nestingDatabaseSchema,
      nestingTable = nestingTable,
      tempEmulationSchema = tempEmulationSchema,
      workFolder = workFolder
    )
  } else {
    stop(paste("Unknow reference set:", referenceSet))
  }
}

createOhdsiDevelopmentNegativeControlCohorts <- function(connectionDetails,
                                                         cdmDatabaseSchema,
                                                         exposureDatabaseSchema,
                                                         exposureTable,
                                                         outcomeDatabaseSchema,
                                                         outcomeTable,
                                                         nestingDatabaseSchema,
                                                         nestingTable,
                                                         tempEmulationSchema,
                                                         workFolder) {
  if (!file.exists(workFolder)) {
    dir.create(workFolder, recursive = TRUE)
  }
  ohdsiDevelopmentNegativeControls <- readRDS(system.file("ohdsiDevelopmentNegativeControls.rds",
    package = "MethodEvaluation"
  ))

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateCohortTable.sql",
    packageName = "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  if (outcomeDatabaseSchema != nestingDatabaseSchema | outcomeTable != nestingTable) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CreateCohortTable.sql",
      packageName = "MethodEvaluation",
      dbms = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = nestingDatabaseSchema,
      cohort_table = nestingTable
    )
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  if (outcomeDatabaseSchema != exposureDatabaseSchema | outcomeTable != exposureTable) {
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = "CreateCohortTable.sql",
      packageName = "MethodEvaluation",
      dbms = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema,
      cohort_database_schema = exposureDatabaseSchema,
      cohort_table = exposureTable
    )
    DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  }
  exposureCohorts <- data.frame(
    cohortName = c("ace_inhibitors", "thiazides_diuretics"),
    cohortId = c(1, 2)
  )
  for (i in 1:nrow(exposureCohorts)) {
    ParallelLogger::logInfo(paste("Creating exposure cohort:", exposureCohorts$cohortName[i]))
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = paste0(exposureCohorts$cohortName[i], ".sql"),
      packageName = "MethodEvaluation",
      dbms = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = cdmDatabaseSchema,
      target_database_schema = exposureDatabaseSchema,
      target_cohort_table = exposureTable,
      target_cohort_id = exposureCohorts$cohortId[i]
    )
    DatabaseConnector::executeSql(connection, sql)
  }
  ParallelLogger::logInfo("Creating negative control outcomes")
  outcomeCohorts <- ohdsiDevelopmentNegativeControls %>%
    distinct(cohortId = .data$outcomeId, cohortName = .data$outcomeName)
  sql <- SqlRender::loadRenderTranslateSql("NegativeControls.sql",
    "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    target_database_schema = outcomeDatabaseSchema,
    target_cohort_table = outcomeTable,
    outcome_ids = outcomeCohorts$cohortId
  )
  DatabaseConnector::executeSql(connection, sql)

  ParallelLogger::logInfo("Creating nesting cohorts")
  nestingCohorts <- ohdsiDevelopmentNegativeControls %>%
    distinct(cohortId = .data$nestingId, cohortName = .data$nestingName)
  sql <- SqlRender::loadRenderTranslateSql("NestingCohorts.sql",
    "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    target_database_schema = nestingDatabaseSchema,
    target_cohort_table = nestingTable,
    nesting_ids = nestingCohorts$cohortId
  )
  DatabaseConnector::executeSql(connection, sql)

  ParallelLogger::logInfo("Counting cohorts")
  exposureCohortCounts <- countCohorts(
    connection = connection,
    cohortDatabaseSchema = exposureDatabaseSchema,
    cohortTable = exposureTable,
    cohortIds = exposureCohorts$cohortId
  ) %>%
    right_join(exposureCohorts, by = "cohortId") %>%
    mutate(type = "Exposure")
  outcomeCohortCounts <- countCohorts(
    connection = connection,
    cohortDatabaseSchema = outcomeDatabaseSchema,
    cohortTable = outcomeTable,
    cohortIds = outcomeCohorts$cohortId
  ) %>%
    right_join(outcomeCohorts, by = "cohortId") %>%
    mutate(type = "Outcome")
  nestingCohortCounts <- countCohorts(
    connection = connection,
    cohortDatabaseSchema = nestingDatabaseSchema,
    cohortTable = nestingTable,
    cohortIds = nestingCohorts$cohortId
  ) %>%
    right_join(nestingCohorts, by = "cohortId") %>%
    mutate(type = "Nesting")
  cohortCounts <- bind_rows(exposureCohortCounts, outcomeCohortCounts, nestingCohortCounts) %>%
    mutate(
      cohortEntries = case_when(
        is.na(.data$cohortEntries) ~ as.integer(0),
        TRUE ~ as.integer(.data$cohortEntries)
      ),
      cohortSubjects = case_when(
        is.na(.data$cohortSubjects) ~ as.integer(0),
        TRUE ~ as.integer(.data$cohortSubjects)
      )
    )
  readr::write_csv(cohortCounts, file.path(workFolder, "cohortCounts.csv"))
  ParallelLogger::logInfo("Cohort counts written to ", file.path(workFolder, "cohortCounts.csv"))
}

createOhdsiNegativeControlCohorts <- function(connectionDetails,
                                              cdmDatabaseSchema,
                                              outcomeDatabaseSchema,
                                              outcomeTable,
                                              nestingDatabaseSchema,
                                              nestingTable,
                                              tempEmulationSchema,
                                              workFolder) {
  ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
    package = "MethodEvaluation"
  ))

  connection <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(connection))

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateCohortTable.sql",
    packageName = "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = outcomeDatabaseSchema,
    cohort_table = outcomeTable
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  sql <- SqlRender::loadRenderTranslateSql(
    sqlFilename = "CreateCohortTable.sql",
    packageName = "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cohort_database_schema = nestingDatabaseSchema,
    cohort_table = nestingTable
  )
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)

  complexOutcomeCohorts <- data.frame(
    sqlName = c("acute_pancreatitis", "gi_bleed", "stroke", "ibd"),
    cohortId = c(1, 2, 3, 4)
  )
  for (i in 1:nrow(complexOutcomeCohorts)) {
    ParallelLogger::logInfo(paste("Creating outcome:", complexOutcomeCohorts$sqlName[i]))
    sql <- SqlRender::loadRenderTranslateSql(
      sqlFilename = paste0(complexOutcomeCohorts$sqlName[i], ".sql"),
      packageName = "MethodEvaluation",
      dbms = connectionDetails$dbms,
      tempEmulationSchema = tempEmulationSchema,
      cdm_database_schema = cdmDatabaseSchema,
      vocabulary_database_schema = cdmDatabaseSchema,
      target_database_schema = outcomeDatabaseSchema,
      target_cohort_table = outcomeTable,
      target_cohort_id = complexOutcomeCohorts$cohortId[i]
    )
    DatabaseConnector::executeSql(connection, sql)
  }
  ParallelLogger::logInfo("Creating other negative control outcomes")
  otherOutcomeCohortIds <- ohdsiNegativeControls %>%
    filter(.data$outcomeId > 4) %>%
    distinct(cohortId = .data$outcomeId) %>%
    pull()
  sql <- SqlRender::loadRenderTranslateSql("NegativeControls.sql",
    "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    target_database_schema = outcomeDatabaseSchema,
    target_cohort_table = outcomeTable,
    outcome_ids = otherOutcomeCohortIds
  )
  DatabaseConnector::executeSql(connection, sql)
  outcomeCohorts <- ohdsiNegativeControls %>%
    distinct(cohortId = .data$outcomeId, cohortName = .data$outcomeName)

  ParallelLogger::logInfo("Creating nesting cohorts")
  nestingCohorts <- ohdsiNegativeControls %>%
    distinct(cohortId = .data$nestingId, cohortName = .data$nestingName)
  sql <- SqlRender::loadRenderTranslateSql("NestingCohorts.sql",
    "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    target_database_schema = nestingDatabaseSchema,
    target_cohort_table = nestingTable,
    nesting_ids = nestingCohorts$cohortId
  )
  DatabaseConnector::executeSql(connection, sql)

  exposureCohorts <- bind_rows(
    ohdsiNegativeControls %>%
      distinct(cohortId = .data$targetId, cohortName = .data$targetName),
    ohdsiNegativeControls %>%
      distinct(cohortId = .data$comparatorId, cohortName = .data$comparatorName)
  ) %>%
    distinct()

  ParallelLogger::logInfo("Counting cohorts")
  exposureCohortCounts <- countDrugEras(
    connection = connection,
    cdmDatabaseSchema = cdmDatabaseSchema,
    cohortIds = exposureCohorts$cohortId
  ) %>%
    right_join(exposureCohorts, by = "cohortId") %>%
    mutate(type = "Exposure")
  outcomeCohortCounts <- countCohorts(
    connection = connection,
    cohortDatabaseSchema = outcomeDatabaseSchema,
    cohortTable = outcomeTable,
    cohortIds = outcomeCohorts$cohortId
  ) %>%
    right_join(outcomeCohorts, by = "cohortId") %>%
    mutate(type = "Outcome")
  nestingCohortCounts <- countCohorts(
    connection = connection,
    cohortDatabaseSchema = nestingDatabaseSchema,
    cohortTable = nestingTable,
    cohortIds = nestingCohorts$cohortId
  ) %>%
    right_join(nestingCohorts, by = "cohortId") %>%
    mutate(type = "Nesting")
  cohortCounts <- bind_rows(exposureCohortCounts, outcomeCohortCounts, nestingCohortCounts) %>%
    mutate(
      cohortEntries = case_when(
        is.na(.data$cohortEntries) ~ as.integer(0),
        TRUE ~ as.integer(.data$cohortEntries)
      ),
      cohortSubjects = case_when(
        is.na(.data$cohortSubjects) ~ as.integer(0),
        TRUE ~ as.integer(.data$cohortSubjects)
      )
    )
  readr::write_csv(cohortCounts, file.path(workFolder, "cohortCounts.csv"))
  ParallelLogger::logInfo("Cohort counts written to ", file.path(workFolder, "cohortCounts.csv"))
}

countCohorts <- function(connection, cohortDatabaseSchema, cohortTable, cohortIds) {
  sql <- "SELECT cohort_definition_id AS cohort_id,
    COUNT(*) AS cohort_entries,
    COUNT(DISTINCT subject_id) AS cohort_subjects
  FROM @cohort_database_schema.@cohort_table
  WHERE cohort_definition_id IN (@cohort_ids)
  GROUP BY cohort_definition_id;"
  cohortCounts <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cohort_database_schema = cohortDatabaseSchema,
    cohort_table = cohortTable,
    cohort_ids = cohortIds,
    snakeCaseToCamelCase = TRUE
  )
  return(cohortCounts)
}

countDrugEras <- function(connection, cdmDatabaseSchema, cohortIds) {
  sql <- "SELECT drug_concept_id AS cohort_id,
    COUNT(*) AS cohort_entries,
    COUNT(DISTINCT person_id) AS cohort_subjects
  FROM @cdm_database_schema.drug_era
  WHERE drug_concept_id IN (@cohort_ids)
  GROUP BY drug_concept_id;"
  cohortCounts <- DatabaseConnector::renderTranslateQuerySql(
    connection = connection,
    sql = sql,
    cdm_database_schema = cdmDatabaseSchema,
    cohort_ids = cohortIds,
    snakeCaseToCamelCase = TRUE
  )
  return(cohortCounts)
}

#' Synthesize positive controls for reference set
#'
#' @details
#' This function will synthesize positive controls for a given reference set based on the real
#' negative controls. Data from the database will be used to fit outcome models for each negative
#' control outcome, and these models will be used to sample additional synthetic outcomes during
#' exposure to increase the true hazard ratio.
#' The positive control outcome cohorts will be stored in the same database table as the negative
#' control outcome cohorts.
#' A summary file will be created listing all positive and negative controls. This list should then be
#' used as input for the method under evaluation.
#'
#' @param connectionDetails        An R object of type \code{ConnectionDetails} created using the
#'                                 function \code{createConnectionDetails} in the
#'                                 \code{DatabaseConnector} package.
#' @param oracleTempSchema    DEPRECATED: use `tempEmulationSchema` instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
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
#'                                 synthesized. Currently supported are "ohdsiMethodsBenchmark" and "ohdsiDevelopment".
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
                                                   oracleTempSchema = NULL,
                                                   tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                                                   cdmDatabaseSchema,
                                                   exposureDatabaseSchema = cdmDatabaseSchema,
                                                   exposureTable = "drug_era",
                                                   outcomeDatabaseSchema = cdmDatabaseSchema,
                                                   outcomeTable = "cohort",
                                                   referenceSet = "ohdsiMethodsBenchmark",
                                                   maxCores = 1,
                                                   workFolder,
                                                   summaryFileName = file.path(workFolder, "allControls.csv")) {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warning("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.")
    tempEmulationSchema <- oracleTempSchema
  }
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
  checkmate::assertChoice(referenceSet, c("ohdsiMethodsBenchmark", "ohdsiDevelopment"), add = errorMessages)
  checkmate::assertInt(maxCores, lower = 1, add = errorMessages)
  checkmate::assertCharacter(workFolder, len = 1, add = errorMessages)
  checkmate::assertCharacter(summaryFileName, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  injectionFolder <- file.path(workFolder, "SignalInjection")
  if (!file.exists(injectionFolder)) {
    dir.create(injectionFolder)
  }

  injectionSummaryFile <- file.path(workFolder, "injectionSummary.rds")
  if (referenceSet == "ohdsiMethodsBenchmark") {
    negativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
      package = "MethodEvaluation"
    ))
  } else {
    negativeControls <- readRDS(system.file("ohdsiDevelopmentNegativeControls.rds",
      package = "MethodEvaluation"
    ))
  }
  if (!file.exists(injectionSummaryFile)) {
    exposureOutcomePairs <- data.frame(
      exposureId = negativeControls$targetId,
      outcomeId = negativeControls$outcomeId
    )
    exposureOutcomePairs <- unique(exposureOutcomePairs)

    prior <- Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE)

    control <- Cyclops::createControl(
      cvType = "auto",
      startingVariance = 0.01,
      noiseLevel = "quiet",
      cvRepetitions = 1,
      threads = min(c(10, maxCores))
    )

    covariateSettings <- FeatureExtraction::createCovariateSettings(
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
    )

    result <- synthesizePositiveControls(connectionDetails,
      cdmDatabaseSchema = cdmDatabaseSchema,
      tempEmulationSchema = tempEmulationSchema,
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
      endAnchor = "cohort end",
      effectSizes = c(1.5, 2, 4),
      precision = 0.01,
      prior = prior,
      control = control,
      maxSubjectsForModel = 250000,
      minOutcomeCountForModel = 100,
      minOutcomeCountForInjection = 25,
      workFolder = injectionFolder,
      modelThreads = max(1, round(maxCores / 8)),
      generationThreads = min(3, maxCores),
      covariateSettings = covariateSettings
    )
    saveRDS(result, injectionSummaryFile)
  }
  injectedSignals <- readRDS(injectionSummaryFile)
  injectedSignals$targetId <- injectedSignals$exposureId
  injectedSignals <- merge(injectedSignals, negativeControls)
  injectedSignals <- injectedSignals[injectedSignals$trueEffectSize != 0, ]
  injectedSignals$outcomeName <- paste0(
    injectedSignals$outcomeName,
    ", RR=",
    injectedSignals$targetEffectSize
  )
  injectedSignals$oldOutcomeId <- injectedSignals$outcomeId
  injectedSignals$outcomeId <- injectedSignals$newOutcomeId
  negativeControls$targetEffectSize <- 1
  negativeControls$trueEffectSize <- 1
  negativeControls$trueEffectSizeFirstExposure <- 1
  negativeControls$oldOutcomeId <- negativeControls$outcomeId
  allControls <- rbind(negativeControls, injectedSignals[, names(negativeControls)])
  exposureOutcomes <- data.frame()
  exposureOutcomes <- rbind(exposureOutcomes, data.frame(
    exposureId = allControls$targetId,
    outcomeId = allControls$outcomeId
  ))
  exposureOutcomes <- rbind(exposureOutcomes, data.frame(
    exposureId = allControls$comparatorId,
    outcomeId = allControls$outcomeId
  ))
  exposureOutcomes <- unique(exposureOutcomes)
  mdrr <- computeMdrr(
    connectionDetails = connectionDetails,
    cdmDatabaseSchema = cdmDatabaseSchema,
    tempEmulationSchema = tempEmulationSchema,
    exposureOutcomePairs = exposureOutcomes,
    exposureDatabaseSchema = exposureDatabaseSchema,
    exposureTable = exposureTable,
    outcomeDatabaseSchema = outcomeDatabaseSchema,
    outcomeTable = outcomeTable,
    cdmVersion = "5"
  )
  allControls <- merge(allControls, data.frame(
    targetId = mdrr$exposureId,
    outcomeId = mdrr$outcomeId,
    mdrrTarget = mdrr$mdrr
  ))
  allControls <- merge(allControls, data.frame(
    comparatorId = mdrr$exposureId,
    outcomeId = mdrr$outcomeId,
    mdrrComparator = mdrr$mdrr
  ), all.x = TRUE)
  readr::write_csv(allControls, summaryFileName)
  ParallelLogger::logInfo("Positive and negative control summary written to ", summaryFileName)
}
