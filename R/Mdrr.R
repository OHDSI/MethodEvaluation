# @file Mdrr.R
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

#' @title
#' Compute minimal detectable relative risk (MDRR)
#'
#' @description
#' \code{computeMdrr} computes the minimal detectable relative risk (MDRR) for drug-outcome pairs.
#'
#'
#' @details
#' Computes the MDRR using simple power-calculations using person-level statistics stratified by age
#' and gender.
#'
#' @param connectionDetails                An R object of type \code{ConnectionDetails} created using
#'                                         the function \code{createConnectionDetails} in the
#'                                         \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema                Name of database schema that contains OMOP CDM and
#'                                         vocabulary.
#' @param oracleTempSchema                 For Oracle only: the name of the database schema where you
#'                                         want all temporary tables to be managed. Requires
#'                                         create/insert permissions to this database.
#' @param exposureOutcomePairs             A data frame with at least two columns:
#'                                         \itemize{
#'                                           \item {"exposureConceptId" containing the drug_concept_ID
#'                                                 or cohort_definition_id of the exposure variable}
#'                                           \item {"outcomeConceptId" containing the
#'                                                 condition_concept_ID or cohort_definition_id of the
#'                                                 outcome variable}
#'                                         }
#'
#' @param exposureDatabaseSchema           The name of the database schema that is the location where
#'                                         the exposure data used to define the exposure cohorts is
#'                                         available.  If exposureTable = DRUG_ERA,
#'                                         exposureDatabaseSchema is not used by assumed to be
#'                                         cdmSchema.  Requires read permissions to this database.
#' @param exposureTable                    The tablename that contains the exposure cohorts.  If
#'                                         exposureTable <> DRUG_ERA, then expectation is exposureTable
#'                                         has format of COHORT table: COHORT_DEFINITION_ID,
#'                                         SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema            The name of the database schema that is the location where
#'                                         the data used to define the outcome cohorts is available.
#'                                         If exposureTable = CONDITION_ERA, exposureDatabaseSchema is
#'                                         not used by assumed to be cdmSchema.  Requires read
#'                                         permissions to this database.
#' @param outcomeTable                     The tablename that contains the outcome cohorts.  If
#'                                         outcomeTable <> CONDITION_OCCURRENCE, then expectation is
#'                                         outcomeTable has format of COHORT table:
#'                                         COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE,
#'                                         COHORT_END_DATE.
#' @param outcomeConditionTypeConceptIds   A list of TYPE_CONCEPT_ID values that will restrict
#'                                         condition occurrences.  Only applicable if outcomeTable =
#'                                         CONDITION_OCCURRENCE.
#'
#' @return
#' A data frame containing the MDRRs for the given exposure-outcome pairs.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "sql server",
#'                                              server = "RNDUSRDHIT07.jnj.com")
#' exposureOutcomePairs <- data.frame(exposureConceptId = c(767410, 1314924, 907879),
#'                                    outcomeConceptId = c(444382, 79106, 138825))
#' mdrrs <- computeMdrr(connectionDetails,
#'                      "cdm_truven_mdcr",
#'                      exposureOutcomePairs,
#'                      outcomeTable = "condition_era")
#' }
#' @export
computeMdrr <- function(connectionDetails,
                        cdmDatabaseSchema,
                        oracleTempSchema = cdmDatabaseSchema,
                        exposureOutcomePairs,
                        exposureDatabaseSchema = cdmDatabaseSchema,
                        exposureTable = "drug_era",
                        outcomeDatabaseSchema = cdmDatabaseSchema,
                        outcomeTable = "condition_era",
                        outcomeConditionTypeConceptIds = c()) {
  cdmDatabase <- strsplit(cdmDatabaseSchema, "\\.")[[1]][1]
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
    exposureConceptId <- "cohort_concept_id"
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
    outcomeConceptId <- "cohort_concept_id"
    outcomePersonId <- "subject_id"
  }

  conn <- connect(connectionDetails)


  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema,
                                                   cdm_database = cdmDatabase,
                                                   exposures_of_interest = unique(exposureOutcomePairs$exposureConceptId),
                                                   outcomes_of_interest = unique(exposureOutcomePairs$outcomeConceptId),
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
                                                   outcome_person_id = outcomePersonId,
                                                   outcome_condition_type_concept_id_list = outcomeConditionTypeConceptIds)

  writeLines("Computing minimumum detectable relative risks. This could take a while")
  DatabaseConnector::executeSql(conn, renderedSql)

  sql <- "SELECT * FROM #mdrr"
  sql <- SqlRender::translateSql(sql,
                                 targetDialect = connectionDetails$dbms,
                                 oracleTempSchema = oracleTempSchema)$sql
  mdrr <- DatabaseConnector::querySql(conn, sql)

  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR_Drop_temp_tables.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(conn, renderedSql, progressBar = FALSE, reportOverallTime = FALSE)

  RJDBC::dbDisconnect(conn)

  names(mdrr) <- toupper(names(mdrr))
  mdrr <- data.frame(exposureConceptId = mdrr$DRUG_CONCEPT_ID,
                     outcomeConceptId = mdrr$CONDITION_CONCEPT_ID,
                     expectedCount = mdrr$EXPECTED_COUNT,
                     mdrr = mdrr$MDRR)
  mdrr <- merge(exposureOutcomePairs, mdrr)
  return(mdrr)
}

#' Filter data based on MDRR
#'
#' @description
#' Filters a dataset to those exposure-outcome pairs with sufficient power.
#'
#' @param data        A data frame with at least two columns:
#'                    \itemize{
#'                      \item {"exposureConceptId" containing the drug_concept_ID or
#'                            cohort_definition_id of the exposure variable}
#'                      \item {"outcomeConceptId" containing the condition_concept_ID or
#'                            cohort_definition_id of the outcome variable}
#'                    }
#'
#' @param mdrr        A data frame as generated by the \code{\link{computeMdrr}} function.
#' @param threshold   The required minimum detectable relative risk.
#'
#' @return
#' A subset of the data object.
#'
#' @export
filterOnMdrr <- function(data, mdrr, threshold = 1.25) {
  dataSubset <- subset(mdrr, mdrr <= threshold, select = c("exposureConceptId", "outcomeConceptId"))
  return(merge(data, dataSubset, by = c("exposureConceptId", "outcomeConceptId")))
}
