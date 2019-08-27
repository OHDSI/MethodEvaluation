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

#' Check source codes used in a cohort definition
#' 
#' @description 
#' This function first extracts all concept sets used in a cohort definition. Then, for each concept set
#' the concept found in the CDM database the contributing source codes are identified. OVerall and per month counts
#' are computed and shown for all concept sets, concepts, and source codes in a HTML table.
#' 
#' This function requires the rmarkdown package as well as PanDoc.
#' 
#' @param connectionDetails    An object of type \code{connectionDetails} as created using the
#'                             \code{\link[DatabaseConnector]{createConnectionDetails}} function in the
#'                             DatabaseConnector package.
#' @param cdmDatabaseSchema    Schema name where your patient-level data in OMOP CDM format resides.
#'                             Note that for SQL Server, this should include both the database and
#'                             schema name, for example 'cdm_data.dbo'.
#' @param oracleTempSchema     Should be used in Oracle to specify a schema where the user has write
#'                             priviliges for storing temporary tables.
#' @param cohortJson           A characteric string containing the JSON of a cohort definition.
#' @param cohortSql            The OHDSI SQL representation of the same cohort definition.
#' @param outputFile           The name of the HTML file to create.
#'
#' @export
checkCohortSourceCodes <- function(connectionDetails,
                                   cdmDatabaseSchema,
                                   oracleTempSchema = NULL,
                                   cohortJson,
                                   cohortSql,
                                   outputFile) {
  if (!is.character(cohortJson)) {
    stop("cohortJson should be character (a JSON string).") 
  }
  # outputFile <- "c:/temp/report.html"
  
  cohortDefinition <- RJSONIO::fromJSON(cohortJson)
  
  connection <- DatabaseConnector::connect(connectionDetails) 
  on.exit(DatabaseConnector::disconnect(connection))
  
  ParallelLogger::logInfo("Instantiating concept sets")
  sql <- gsub("with primary_events.*", "", cohortSql)   
  sql <- SqlRender::render(sql, vocabulary_database_schema = cdmDatabaseSchema)
  sql <- SqlRender::translate(sql,
                              targetDialect = connectionDetails$dbms,
                              oracleTempSchema = oracleTempSchema)
  DatabaseConnector::executeSql(connection, sql)
  
  ParallelLogger::logInfo("Counting codes in concept sets")
  sql <- SqlRender::loadRenderTranslateSql("CohortSourceCodes.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema)
  counts <- DatabaseConnector::querySql(connection, sql)
  colnames(counts) <- SqlRender::snakeCaseToCamelCase(colnames(counts))

  sql <- SqlRender::loadRenderTranslateSql("ObservedPerCalendarMonth.sql",
                                           packageName = "MethodEvaluation",
                                           dbms = connectionDetails$dbms,
                                           oracleTempSchema = oracleTempSchema,
                                           cdm_database_schema = cdmDatabaseSchema)
  backgroundCounts <- DatabaseConnector::querySql(connection, sql)
  colnames(backgroundCounts) <- SqlRender::snakeCaseToCamelCase(colnames(backgroundCounts))
  backgroundCounts$startCount[is.na(backgroundCounts$startCount)] <- 0
  backgroundCounts$endCount[is.na(backgroundCounts$endCount)] <- 0
  backgroundCounts$net <- backgroundCounts$startCount - backgroundCounts$endCount
  backgroundCounts$time <- backgroundCounts$eventYear + (backgroundCounts$eventMonth - 1) / 12
  backgroundCounts <- backgroundCounts[order(backgroundCounts$time), ]
  backgroundCounts$backgroundCount <- cumsum(backgroundCounts$net)
  backgroundCounts$backgroundCount <- backgroundCounts$backgroundCount + backgroundCounts$endCount
  
  counts <- merge(counts, backgroundCounts[, c("eventYear", "eventMonth", "backgroundCount")], all.x = TRUE)
  if (any(is.na(counts$backgroundCount))) {
    stop("code counts in calendar months without observation period starts or ends. Need to do some lookup here") 
  }
  counts$proportion <- counts$personCount / counts$backgroundCount
  counts <- counts[order(counts$codesetId, 
                         counts$conceptId, 
                         counts$sourceConceptName, 
                         counts$sourceVocabularyId,
                         counts$eventYear,
                         counts$eventMonth), ]
  
  countsFile <- tempfile()
  saveRDS(counts, countsFile)
  cohortDefinitionFile <- tempfile()
  saveRDS(cohortDefinition, cohortDefinitionFile)
  
  fullPath <- normalizePath(outputFile, mustWork = FALSE)
  rmarkdown::render(system.file("rMarkDown", "CohortSourceCodes.Rmd", package = "MethodEvaluation"),
                    output_dir = dirname(fullPath),
                    output_file = basename(fullPath),
                    params = list(countsFile = countsFile,
                                  cohortDefinitionFile = cohortDefinitionFile))
  
  unlink(countsFile)
  unlink(cohortDefinitionFile)
  invisible(NULL)
}
