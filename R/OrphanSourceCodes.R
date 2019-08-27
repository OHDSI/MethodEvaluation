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

#' Find source codes that do not roll up to their ancestor
#' 
#' @details 
#' Searches for concepts where the name contains the concept name of interest, or any of its synonyms as provided by the user. 
#' Then it checks whether these concepts roll up to the main concept.
#'
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param conceptName          The exact name of the parent concept.
#' @param conceptSynonyms      Synonyms by which the code might be described.
#'
#' @return 
#' A data frame with orhan source codes, with counts per domain how often the code was encountered
#' in the CDM.
#' 
#' @export
findOrphanSourceCodes <- function(connectionDetails,
                                  cdmDatabaseSchema,
                                  oracleTempSchema = NULL,
                                  conceptName,
                                  conceptSynonyms = NULL) {
  start <- Sys.time()
  connection <- DatabaseConnector::connect(connectionDetails)
  
  names <- c(conceptName, conceptSynonyms)
  conceptNameClause <- paste(paste0("concept_name LIKE '%", names, "%'"), collapse = " OR\n")
  sourceCodeDescriptionClause <- paste(paste0("source_code_description LIKE '%", names, "%'"), collapse = " OR\n")
  
  sql <- SqlRender::loadRenderTranslateSql("PrepareOrphanCodeSearch.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema,
                                           concept_name = conceptName,
                                           concept_name_clause = conceptNameClause,
                                           source_code_description_clause = sourceCodeDescriptionClause)
  DatabaseConnector::executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  sql <- SqlRender::loadRenderTranslateSql("OrphanCodeSearch.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema)
  
  orphanCodes <- DatabaseConnector::querySql(connection, sql)
  colnames(orphanCodes) <- SqlRender::snakeCaseToCamelCase(colnames(orphanCodes))
  
  DatabaseConnector::disconnect(connection)
  
  orphanCodes$overallCount <- orphanCodes$drugCount +
    orphanCodes$conditionCount +
    orphanCodes$procedureCount +
    orphanCodes$deviceCount +
    orphanCodes$measurementCount +
    orphanCodes$observationCount

  orphanCodes <- orphanCodes[order(-orphanCodes$overallCount), ]
  delta <- Sys.time() - start
  writeLines(paste("Finding orphan codes took", signif(delta, 3), attr(delta, "units")))
  return(orphanCodes)
}
