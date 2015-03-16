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
#' This function will insert additional outcomes for a given set of drug-outcome pairs. It is 
#' assumed that these drug-outcome pairs represent negative controls, so the true relative risk 
#' before inserting any outcomes should be 1. There are several models for inserting the outcomes
#' during the specified risk window of the drug.
#' 
#' @param connectionDetails  An R object of type \code{ConnectionDetails} created using the function \code{createConnectionDetails} in the \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema      Name of database schema that contains OMOP CDM and vocabulary.
#' @param oracleTempSchema    For Oracle only: the name of the database schema where you want all temporary tables to be managed. Requires create/insert permissions to this database. 
#' @param exposureOutcomePairs  A data frame with at least two columns:
#' \itemize{
#'   \item{"exposureConceptId" containing the drug_concept_ID or cohort_definition_id of the exposure variable}
#'   \item{"outcomeConceptId" containing the condition_concept_ID or cohort_definition_id of the outcome variable}
#' }
#' @param exposureDatabaseSchema     The name of the database schema that is the location where the exposure data used to define the exposure cohorts is available.  If exposureTable = DRUG_ERA, exposureDatabaseSchema is not used by assumed to be cdmSchema.  Requires read permissions to this database.    
#' @param exposureTable   The tablename that contains the exposure cohorts.  If exposureTable <> DRUG_ERA, then expectation is exposureTable has format of COHORT table: COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.  
#' @param outcomeDatabaseSchema     The name of the database schema that is the location where the data used to define the outcome cohorts is available.  If exposureTable = CONDITION_ERA, exposureDatabaseSchema is not used by assumed to be cdmSchema.  Requires read permissions to this database.    
#' @param outcomeTable   The tablename that contains the outcome cohorts.  If outcomeTable <> CONDITION_OCCURRENCE, then expectation is outcomeTable has format of COHORT table: COHORT_DEFINITION_ID, SUBJECT_ID, COHORT_START_DATE, COHORT_END_DATE.   
#' @param outcomeConditionTypeConceptIds   A list of TYPE_CONCEPT_ID values that will restrict condition occurrences.  Only applicable if outcomeTable = CONDITION_OCCURRENCE.
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
                          exposureOutcomePairs,
                          modelType = "poisson",
                          buildOutcomeModel = TRUE,
                          firstExposureOnly = FALSE,
                          washoutWindow = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0, 
                          addExposureDaysToEnd = TRUE,
                          firstOutcomeOnly = TRUE,
                          effectSizes = c(1, 1.25, 1.5, 2, 4, 8)){
  cdmDatabase <- strsplit(cdmDatabaseSchema ,"\\.")[[1]][1]
  exposureConceptIds <- unique(exposureOutcomePairs$exposureConceptId)
  conn <- DatabaseConnector::connect(connectionDetails)
  
  for (exposureConceptId in exposureConceptIds){
    writeLines(paste("Processung drug", exposureConcept))  
    outcomeConceptIds <- unique(exposureOutcomePairs$outcomeConceptId[exposureOutcomePairs$exposureConceptId == exposureConceptId])    
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
    DatabaseConnector::executeSql(conn,renderedSql)
    
    writeLines("\nExtracting risk windows and outcome counts")
    exposureSql <-"SELECT cohort_definition_id, subject_id, cohort_start_date, DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS days_at_risk FROM #cohort_person"
    exposureSql <- SqlRender::translateSql(exposureSql, "sql server", connectionDetails$dbms, oracleTempSchema)$sql
    exposures <- DatabaseConnector::querySql.ffdf(conn, exposureSql)                                                   
    exposure_outcome_pairs <- data.frame(exposure_concept_id = exposureOutcomePairs$exposureConceptId, outcome_concept_id = exposureOutcomePairs$outcomeConceptId)
    DatabaseConnector::dbInsertTable(conn, "#exposure_outcome_pairs", exposure_outcome_pairs, TRUE, TRUE, TRUE, oracleTempSchema)
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
    
    if (buildOutcomeModel){
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
      
      for (outcomeConcept in outcomeConceptIds){
        # Fit model
        for (effectSize in effectSizes){
          # Generate different effect sizes
        }        
      }
    }
    # TODO: remove cohort_person temp table
  }
  dummy <- RJDBC::dbDisconnect(conn)
  list(exposures, outcomeCounts, covariates)  
}
