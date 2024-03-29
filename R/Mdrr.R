# @file Mdrr.R
#
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

#' @title
#' Compute minimal detectable relative risk (MDRR)
#'
#' @description
#' \code{computeMdrr} computes the minimal detectable relative risk (MDRR) for drug-outcome pairs
#' using a standard approach that stratifies by age and gender (Armstrong 1987).
#'
#'
#' @param connectionDetails        An R object of type \code{ConnectionDetails} created using the
#'                                 function \code{createConnectionDetails} in the
#'                                 \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema        Name of database schema that contains OMOP CDM and vocabulary.
#' @param oracleTempSchema    DEPRECATED: use `tempEmulationSchema` instead.
#' @param tempEmulationSchema Some database platforms like Oracle and Impala do not truly support temp tables. To
#'                            emulate temp tables, provide a schema with write privileges where temp tables
#'                            can be created.
#' @param exposureOutcomePairs     A data frame with at least two columns:
#'                                 \itemize{
#'                                   \item {"exposureId" or "targetId" containing the drug_concept_ID or
#'                                         cohort_definition_id of the exposure variable}
#'                                   \item {"outcomeId" containing the condition_concept_ID or
#'                                         cohort_definition_id of the outcome variable}
#'                                 }
#'
#'
#' @param exposureDatabaseSchema   The name of the database schema that is the location where the
#'                                 exposure data used to define the exposure cohorts is available.  If
#'                                 exposureTable = DRUG_ERA, exposureDatabaseSchema is not used by
#'                                 assumed to be cdmSchema.  Requires read permissions to this
#'                                 database.
#' @param exposureTable            The tablename that contains the exposure cohorts.  If exposureTable
#'                                 <> DRUG_ERA, then expectation is exposureTable has format of COHORT
#'                                 table: COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                 COHORT_END_DATE.
#' @param outcomeDatabaseSchema    The name of the database schema that is the location where the data
#'                                 used to define the outcome cohorts is available. If exposureTable =
#'                                 CONDITION_ERA, exposureDatabaseSchema is not used by assumed to be
#'                                 cdmSchema.  Requires read permissions to this database.
#' @param outcomeTable             The tablename that contains the outcome cohorts.  If outcomeTable <>
#'                                 CONDITION_OCCURRENCE, then expectation is outcomeTable has format of
#'                                 COHORT table: COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                 COHORT_END_DATE.
#' @param cdmVersion               Define the OMOP CDM version used: currently support "4" and "5".
#'
#' @references
#' Armstrong B. A simple estimator of minimum detectable relative risk, sample size, or power in
#' cohort studies. American journal of epidemiology. 1987; 126: 356-8.
#'
#' @return
#' A data frame containing the MDRRs for the given exposure-outcome pairs.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(
#'   dbms = "sql server",
#'   server = "RNDUSRDHIT07.jnj.com"
#' )
#' exposureOutcomePairs <- data.frame(
#'   exposureId = c(767410, 1314924, 907879),
#'   outcomeId = c(444382, 79106, 138825)
#' )
#' mdrrs <- computeMdrr(connectionDetails,
#'   "cdm_truven_mdcr",
#'   exposureOutcomePairs,
#'   outcomeTable = "condition_era"
#' )
#' }
#' @export
computeMdrr <- function(connectionDetails,
                        oracleTempSchema = NULL,
                        tempEmulationSchema = getOption("sqlRenderTempEmulationSchema"),
                        cdmDatabaseSchema,
                        exposureOutcomePairs,
                        exposureDatabaseSchema = cdmDatabaseSchema,
                        exposureTable = "drug_era",
                        outcomeDatabaseSchema = cdmDatabaseSchema,
                        outcomeTable = "condition_era",
                        cdmVersion = "5") {
  if (!is.null(oracleTempSchema) && oracleTempSchema != "") {
    warning("The 'oracleTempSchema' argument is deprecated. Use 'tempEmulationSchema' instead.")
    tempEmulationSchema <- oracleTempSchema
  }
  if (is.null(exposureOutcomePairs$exposureId) && !is.null(exposureOutcomePairs$targetId)) {
    exposureOutcomePairs$exposureId <- exposureOutcomePairs$targetId
  }
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertClass(connectionDetails, "ConnectionDetails", add = errorMessages)
  checkmate::assertCharacter(tempEmulationSchema, len = 1, null.ok = TRUE, add = errorMessages)
  checkmate::assertCharacter(cdmDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertDataFrame(exposureOutcomePairs, add = errorMessages)
  checkmate::assertNames(colnames(exposureOutcomePairs), must.include = c("exposureId", "outcomeId"), add = errorMessages)
  checkmate::assertCharacter(exposureDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(exposureTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeDatabaseSchema, len = 1, add = errorMessages)
  checkmate::assertCharacter(outcomeTable, len = 1, add = errorMessages)
  checkmate::assertCharacter(cdmVersion, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  exposureTable <- tolower(exposureTable)
  outcomeTable <- tolower(outcomeTable)
  if (exposureTable == "drug_era") {
    exposureStartDate <- "drug_era_start_date"
    exposureEndDate <- "drug_era_end_date"
    exposureConceptId <- "drug_concept_id"
    exposurePersonId <- "person_id"
  } else {
    exposureStartDate <- "cohort_start_date"
    exposureEndDate <- "cohort_end_date"
    if (cdmVersion == "4") {
      exposureConceptId <- "cohort_concept_id"
    } else {
      exposureConceptId <- "cohort_definition_id"
    }
    exposurePersonId <- "subject_id"
  }

  if (outcomeTable == "condition_era") {
    outcomeStartDate <- "condition_era_start_date"
    outcomeEndDate <- "condition_era_end_date"
    outcomeConceptId <- "condition_concept_id"
    outcomePersonId <- "person_id"
  } else if (outcomeTable == "condition_occurrence") {
    outcomeStartDate <- "condition_start_date"
    outcomeEndDate <- "condition_end_date"
    outcomeConceptId <- "condition_concept_id"
    outcomePersonId <- "person_id"
  } else {
    outcomeStartDate <- "cohort_start_date"
    outcomeEndDate <- "cohort_end_date"
    if (cdmVersion == "4") {
      outcomeConceptId <- "cohort_concept_id"
    } else {
      outcomeConceptId <- "cohort_definition_id"
    }
    outcomePersonId <- "subject_id"
  }

  conn <- DatabaseConnector::connect(connectionDetails)
  on.exit(DatabaseConnector::disconnect(conn))

  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR.sql",
    packageName = "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema,
    cdm_database_schema = cdmDatabaseSchema,
    exposures_of_interest = unique(exposureOutcomePairs$exposureId),
    outcomes_of_interest = unique(exposureOutcomePairs$outcomeId),
    exposure_database_schema = exposureDatabaseSchema,
    exposure_table = exposureTable,
    exposure_start_date = exposureStartDate,
    exposure_end_date = exposureEndDate,
    exposure_concept_id = exposureConceptId,
    exposure_person_id = exposurePersonId,
    outcome_database_schema = outcomeDatabaseSchema,
    outcome_table = outcomeTable,
    outcome_start_date = outcomeStartDate,
    outcome_end_date = outcomeEndDate,
    outcome_concept_id = outcomeConceptId,
    outcome_person_id = outcomePersonId
  )

  message("Computing minimumum detectable relative risks. This could take a while")
  DatabaseConnector::executeSql(conn, renderedSql)

  sql <- "SELECT * FROM #mdrr"
  sql <- SqlRender::translate(sql,
    targetDialect = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  mdrr <- DatabaseConnector::querySql(conn, sql, snakeCaseToCamelCase = TRUE)

  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR_Drop_temp_tables.sql",
    packageName = "MethodEvaluation",
    dbms = connectionDetails$dbms,
    tempEmulationSchema = tempEmulationSchema
  )
  DatabaseConnector::executeSql(conn, renderedSql, progressBar = FALSE, reportOverallTime = FALSE)

  mdrr <- data.frame(
    exposureId = mdrr$drugConceptId,
    outcomeId = mdrr$conditionConceptId,
    exposurePersonCount = mdrr$drugPersonCount,
    outcomePersonCount = mdrr$conditionPersonCount,
    personCount = mdrr$personCount,
    expectedCount = mdrr$expectedCount,
    mdrr = mdrr$mdrr
  )
  mdrr <- merge(exposureOutcomePairs, mdrr)
  return(mdrr)
}
