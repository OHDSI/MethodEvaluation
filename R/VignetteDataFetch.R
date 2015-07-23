# @file VignetteDataFetch.R
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

#' @keywords internal
.signalInjectionVignetteDataFetch <- function() {
  # This function should be used to fetch the data that is used in the vignettes.
  # library(SqlRender);library(DatabaseConnector) ;library(MethodEvaluation);library(CohortMethod); setwd('s:/temp');options('fftempdir' = 's:/temp')
  
  pw <- NULL
  dbms <- "sql server"
  user <- NULL
  server <- "RNDUSRDHIT07.jnj.com"
  cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
  resultsDatabaseSchema <- "scratch.dbo"
  port <- NULL
  
  dbms <- "postgresql"
  user <- "postgres"
  server <- "localhost/ohdsi"
  cdmDatabaseSchema <- "vocabulary5"
  resultsDatabaseSchema <- "scratch"
  port <- NULL
  
  pw <- NULL
  dbms <- "pdw"
  user <- NULL
  server <- "JRDUSAPSCTL01"
  cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
  resultsDatabaseSchema <- "scratch.dbo"
  outcomesTable <- "mschuemie_outcomes"
  outputTable <- "mschuemi_injected_signals"
  port <- 17001
  
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  server = server,
                                                                  user = user,
                                                                  password = pw,
                                                                  port = port)
  
  sql <- loadRenderTranslateSql("VignetteOutcomes.sql",
                                packageName = "MethodEvaluation",
                                dbms = dbms,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                resultsDatabaseSchema = resultsDatabaseSchema,
                                outcomesTable = outcomesTable)
  
  connection <- DatabaseConnector::connect(connectionDetails)
  DatabaseConnector::executeSql(connection, sql)
  
  # Check number of subjects per cohort:
  sql <- "SELECT cohort_concept_id, COUNT(*) AS count FROM @resultsDatabaseSchema.@outcomesTable GROUP BY cohort_concept_id"
  sql <- SqlRender::renderSql(sql, resultsDatabaseSchema = resultsDatabaseSchema, outcomesTable = outcomesTable)$sql
  sql <- SqlRender::translateSql(sql, targetDialect = connectionDetails$dbms)$sql
  DatabaseConnector::querySql(connection, sql)
  dbDisconnect(connection)
  
  #Diclofenac and all negative controls:
  exposureOutcomePairs <- data.frame(exposureConceptId = 1124300,
                                     outcomeConceptId = c(24609, 29735, 73754, 80004, 134718, 139099, 141932, 192367, 193739, 194997, 197236, 199074, 255573, 257007, 313459, 314658, 316084, 319843, 321596, 374366, 375292, 380094, 433753, 433811, 436665, 436676, 436940, 437784, 438134, 440358, 440374, 443617, 443800, 4084966, 4288310))
  
  prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
  
  control = createControl(cvType = "auto",
                          startingVariance = 0.1, 
                          noiseLevel = "quiet", 
                          threads = 10)
  
  result <- injectSignals(connectionDetails,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          exposureOutcomePairs = exposureOutcomePairs,
                          exposureTable = "drug_era",
                          outcomeDatabaseSchema = resultsDatabaseSchema,
                          outcomeTable = outcomesTable,
                          outputDatabaseSchema = resultsDatabaseSchema,
                          outputTable = outputTable,
                          createOutputTable = TRUE,
                          firstExposureOnly = TRUE,
                          firstOutcomeOnly = TRUE,
                          modelType = "survival",
                          prior = prior,
                          control = control,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          effectSizes = c(1, 1.25, 1.5, 2, 4),
                          tempFolder = "s:/temp/SignalInjectionTemp")
}
