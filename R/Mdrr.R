# @file Mdrr.R
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

#' @title
#' Compute minimal detectable relative risk (MDRR)
#'
#' @description
#' \code{computeMdrr} computes the minimal detectable relative risk (MDRR) for drug-outcome pairs using a 
#' standard approach that stratifies by age and gender (Armstrong 1987).
#'
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
#'                                           \item {"exposureId" or "targetId" containing the drug_concept_ID
#'                                                 or cohort_definition_id of the exposure variable}
#'                                           \item {"outcomeId" containing the
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
#' @param cdmVersion                   Define the OMOP CDM version used: currently support "4" and "5".
#' 
#' @references 
#' Armstrong B. A simple estimator of minimum detectable relative risk, sample size, or power in cohort studies. 
#' American journal of epidemiology. 1987; 126: 356-8.
#'
#' @return
#' A data frame containing the MDRRs for the given exposure-outcome pairs.
#'
#' @examples
#' \dontrun{
#' connectionDetails <- createConnectionDetails(dbms = "sql server",
#'                                              server = "RNDUSRDHIT07.jnj.com")
#' exposureOutcomePairs <- data.frame(exposureId = c(767410, 1314924, 907879),
#'                                    outcomeId = c(444382, 79106, 138825))
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
                        cdmVersion = "5") {
  if (is.null(exposureOutcomePairs$exposureId) && !is.null(exposureOutcomePairs$targetId))
    exposureOutcomePairs$exposureId <- exposureOutcomePairs$targetId
  if (is.null(exposureOutcomePairs$exposureId))
    stop("exposureOutcomePairs is missing exposureId and targetId column")
  if (is.null(exposureOutcomePairs$outcomeId))
    stop("exposureOutcomePairs is missing outcomeId column")
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
  
  
  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema,
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
                                                   outcome_person_id = outcomePersonId)
  
  writeLines("Computing minimumum detectable relative risks. This could take a while")
  DatabaseConnector::executeSql(conn, renderedSql)
  
  sql <- "SELECT * FROM #mdrr"
  sql <- SqlRender::translateSql(sql,
                                 targetDialect = connectionDetails$dbms,
                                 oracleTempSchema = oracleTempSchema)$sql
  mdrr <- DatabaseConnector::querySql(conn, sql)
  colnames(mdrr) <- SqlRender::snakeCaseToCamelCase(colnames(mdrr))
  
  renderedSql <- SqlRender::loadRenderTranslateSql("MDRR_Drop_temp_tables.sql",
                                                   packageName = "MethodEvaluation",
                                                   dbms = connectionDetails$dbms,
                                                   oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(conn, renderedSql, progressBar = FALSE, reportOverallTime = FALSE)
  
  DatabaseConnector::disconnect(conn)
  
  mdrr <- data.frame(exposureId = mdrr$drugConceptId,
                     outcomeId = mdrr$conditionConceptId,
                     exposurePersonCount = mdrr$drugPersonCount,
                     outcomePersonCount = mdrr$conditionPersonCount,
                     personCount = mdrr$personCount,
                     expectedCount = mdrr$expectedCount,
                     mdrr = mdrr$mdrr)
  mdrr <- merge(exposureOutcomePairs, mdrr)
  return(mdrr)
}
