# Copyright 2017 Observational Health Data Sciences and Informatics
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
#' This function will create the outcomes of interest and nesting cohorts referenced in the 
#' various reference sets. The outcomes of interest are derives using information like 
#' diagnoses, procedures, and drug prescriptions. The outcomes are stored in a table on 
#' the database server.
#'
#' @param connectionDetails      An R object of type \code{ConnectionDetails} created using the
#'                               function \code{createConnectionDetails} in the
#'                               \code{DatabaseConnector} package.
#' @param oracleTempSchema       Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.                               
#' @param cdmDatabaseSchema      A database schema containing health care data in the OMOP Commond Data
#'                               Model. Note that for SQL Server, botth the database and schema should
#'                               be specified, e.g. 'cdm_schema.dbo'
#' @param outcomeDatabaseSchema   The database schema where the target outcome table is located. Note that for
#'                               SQL Server, both the database and schema should be specified, e.g.
#'                               'cdm_schema.dbo'
#' @param outcomeTable            The name of the table where the outcomes will be stored.
#' @param nestingDatabaseSchema   The database schema where the nesting outcome table is located. Note that for
#'                               SQL Server, both the database and schema should be specified, e.g.
#'                               'cdm_schema.dbo'. (For the OHDSI negative controls only)
#' @param nestingTable            The name of the table where the nesting cohorts will be stored.
#'                               (For the OHDSI negative controls only)
#' @param referenceSet           The name of the reference set for which outcomes need to be created.
#'                               Currently supported are "omopReferenceSet", "euadrReferenceSet", and
#'                               "ohdsiNegativeControls".
#'
#' @export
createReferenceSetCohorts <- function(connectionDetails,
                                      oracleTempSchema = NULL,
                                      cdmDatabaseSchema,
                                      outcomeDatabaseSchema = cdmDatabaseSchema,
                                      outcomeTable = "outcomes",
                                      nestingDatabaseSchema = cdmDatabaseSchema,
                                      nestingTable = "nesting",
                                      referenceSet = "omopReferenceSet") {
  
  if (referenceSet == "omopReferenceSet") {
    writeLines("Generating HOIs for the OMOP reference set")
    renderedSql <- SqlRender::loadRenderTranslateSql("CreateOmopHois.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     outcome_database_schema = outcomeDatabaseSchema,
                                                     outcome_table = outcomeTable)
    conn <- DatabaseConnector::connect(connectionDetails)
    DatabaseConnector::executeSql(conn, renderedSql)
    writeLines("Done")
    DatabaseConnector::disconnect(conn)
  } else if (referenceSet == "euadrReferenceSet") {
    writeLines("Generating HOIs for the EU-ADR reference set")
    # TODO: add code for creating the EU-ADR HOIs
  } else if (referenceSet == "ohdsiNegativeControls") {
    writeLines("Generating HOIs and nesting cohorts for the OHDSI negative control set")
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
  ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds", package = "MethodEvaluation"))
  
  connection <- DatabaseConnector::connect(connectionDetails)
  
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
                                id = c(1,2,3,4))
  for (i in 1:nrow(cohortsToCreate)) {
    writeLines(paste("Creating outcome:", cohortsToCreate$name[i]))
    sql <- SqlRender::loadRenderTranslateSql(sqlFilename = paste0(cohortsToCreate$name[i], ".sql"),
                                             packageName = "MethodEvaluation",
                                             dbms = connectionDetails$dbms,
                                             oracleTempSchema = oracleTempSchema,
                                             cdm_database_schema = cdmDatabaseSchema,
                                             target_database_schema = outcomeDatabaseSchema,
                                             target_cohort_table = outcomeTable,
                                             target_cohort_id = cohortsToCreate$id[i])
    DatabaseConnector::executeSql(connection, sql)
  } 
  writeLines("Creating other negative control outcomes")
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
  
  writeLines("Creating nesting cohorts")
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
  
}
