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
  # library(SqlRender);library(DatabaseConnector) ;library(MethodEvaluation);library(CohortMethod); setwd('s:/temp');options('fftempdir' = 's:/fftemp')
  
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
  exposureOutcomePairs <- data.frame(exposureId = 1124300,
                                     outcomeId = c(24609, 29735, 73754, 80004, 134718, 139099, 141932, 192367, 193739, 194997, 197236, 199074, 255573, 257007, 313459, 314658, 316084, 319843, 321596, 374366, 375292, 380094, 433753, 433811, 436665, 436676, 436940, 437784, 438134, 440358, 440374, 443617, 443800, 4084966, 4288310))
  
  prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
  
  control = createControl(cvType = "auto",
                          startingVariance = 0.001, 
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
  
  saveRDS(result, "s:/temp/SignalInjectionSummary.rds")
  # result <- readRDS("s:/temp/SignalInjectionSummary.rds")

  library(CohortMethod)
  resultSum <- result$summary[result$summary$trueEffectSize != 0,]
  dcos <- createDrugComparatorOutcomes(targetId = 1124300, comparatorId = 1118084, outcomeIds = resultSum$newOutcomeId)
  drugComparatorOutcomesList <- list(dcos)
  
  covarSettings <- createCovariateSettings(useCovariateDemographics = TRUE,
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
                                           useCovariateObservationCount365d = TRUE,
                                           useCovariateMeasurement365d = TRUE,
                                           useCovariateMeasurement30d = TRUE,
                                           useCovariateMeasurementCount365d = TRUE,
                                           useCovariateMeasurementBelow = TRUE,
                                           useCovariateMeasurementAbove = TRUE,
                                           useCovariateConceptCounts = TRUE,
                                           useCovariateRiskScores = TRUE,
                                           useCovariateRiskScoresCharlson = TRUE,
                                           useCovariateRiskScoresDCSI = TRUE,
                                           useCovariateRiskScoresCHADS2 = TRUE,
                                           useCovariateInteractionYear = FALSE,
                                           useCovariateInteractionMonth = FALSE,
                                           excludedCovariateConceptIds = c(),
                                           deleteCovariatesSmallCount = 100)
  getDbCmDataArgs <- createGetDbCohortMethodDataArgs(washoutWindow = 183,
                                                     indicationLookbackWindow = 183,
                                                     studyStartDate = "",
                                                     studyEndDate = "",
                                                     excludeDrugsFromCovariates = TRUE,
                                                     covariateSettings = covarSettings)
  fitOutcomeModelArgs1 <- createFitOutcomeModelArgs(riskWindowStart = 0,
                                                    riskWindowEnd = 0,
                                                    addExposureDaysToEnd = TRUE,
                                                    useCovariates = FALSE,
                                                    modelType = "cox",
                                                    stratifiedCox = FALSE)
 cmAnalysis1 <- createCmAnalysis(analysisId = 1,
                                  description = "No matching, simple outcome model",
                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                  fitOutcomeModel = TRUE,
                                  fitOutcomeModelArgs = fitOutcomeModelArgs1)
  createPsArgs <- createCreatePsArgs() # Using only defaults
  matchOnPsArgs <- createMatchOnPsArgs(maxRatio = 100)
  fitOutcomeModelArgs2 <- createFitOutcomeModelArgs(riskWindowStart = 0,
                                                    riskWindowEnd = 0,
                                                    addExposureDaysToEnd = TRUE,
                                                    useCovariates = FALSE,
                                                    modelType = "cox",
                                                    stratifiedCox = TRUE)
  cmAnalysis2 <- createCmAnalysis(analysisId = 2,
                                  description = "Matching plus stratified outcome model",
                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                  createPs = TRUE,
                                  createPsArgs = createPsArgs,
                                  matchOnPs = TRUE,
                                  matchOnPsArgs = matchOnPsArgs,
                                  fitOutcomeModel = TRUE,
                                  fitOutcomeModelArgs = fitOutcomeModelArgs2)
  fitOutcomeModelArgs3 <- createFitOutcomeModelArgs(riskWindowStart = 0,
                                                    riskWindowEnd = 0,
                                                    addExposureDaysToEnd = TRUE,
                                                    useCovariates = TRUE,
                                                    modelType = "cox",
                                                    stratifiedCox = TRUE)
  cmAnalysis3 <- createCmAnalysis(analysisId = 3,
                                  description = "Matching plus full outcome model",
                                  getDbCohortMethodDataArgs = getDbCmDataArgs,
                                  createPs = TRUE,
                                  createPsArgs = createPsArgs,
                                  matchOnPs = TRUE,
                                  matchOnPsArgs = matchOnPsArgs,
                                  fitOutcomeModel = TRUE,
                                  fitOutcomeModelArgs = fitOutcomeModelArgs3)
  cmAnalysisList <- list(cmAnalysis1, cmAnalysis2, cmAnalysis3)
  
  saveCmAnalysisList(cmAnalysisList, "s:/temp/SignalInjectionCmAnalysisList.txt")
  saveDrugComparatorOutcomesList(drugComparatorOutcomesList, "s:/temp/SignalInjectionDrugComparatorOutcomesList.txt")
  
  # cmAnalysisList <- loadCmAnalysisList("s:/temp/SignalInjectionCmAnalysisList.txt")
  # drugComparatorOutcomesList <- loadDrugComparatorOutcomesList("s:/temp/SignalInjectionDrugComparatorOutcomesList.txt")
    
  cmResult <- runCmAnalyses(connectionDetails = connectionDetails, 
                          cdmDatabaseSchema = cdmDatabaseSchema, 
                          exposureTable = "drug_era", 
                          outcomeDatabaseSchema = resultsDatabaseSchema, 
                          outcomeTable = outputTable, 
                          outputFolder = "s:/temp/SignalInjectionCohortMethodOutput", 
                          cmAnalysisList = cmAnalysisList, 
                          drugComparatorOutcomesList = drugComparatorOutcomesList, 
                          getDbCohortMethodDataThreads = 1, 
                          createPsThreads = 1, 
                          psCvThreads = 10, 
                          trimMatchStratifyThreads = 10, 
                          computeCovarBalThreads = 2, 
                          fitOutcomeModelThreads = 3, 
                          outcomeCvThreads = 10)

  setwd("s:/temp")
  signalInjSum <- readRDS("s:/temp/SignalInjectionSummary.rds")
  signalInjResultSum <- signalInjSum$summary[signalInjSum$summary$trueEffectSize != 0,]
  cmResult <- readRDS("s:/temp/SignalInjectionCohortMethodOutput/outcomeModelReference.rds")
  cmAnalysisSum <- summarizeAnalyses(cmResult)
  estimates <- cmAnalysisSum[!is.infinite(cmAnalysisSum$seLogRr) & !is.na(cmAnalysisSum$seLogRr),]
  estimates <- merge(estimates, signalInjResultSum[,c("targetEffectSize","newOutcomeId")], by.x = "outcomeId", by.y = "newOutcomeId")
  estimates$trueLogRr <- log(estimates$targetEffectSize)
  library(EmpiricalCalibration)
  
  estimatesAnalysis <- estimates[estimates$analysisId == 1,]
  plotTrueAndObserved(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest1.png")
  plotCoverage(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov1.png")

  estimatesAnalysis <- estimates[estimates$analysisId == 2,]
  plotTrueAndObserved(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest2.png")
  plotCoverage(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov2.png")
  
  estimatesAnalysis <- estimates[estimates$analysisId == 3,]
  plotTrueAndObserved(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest3.png")
  plotCoverage(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov3.png")
  
  estimatesAnalysis <- estimates[estimates$analysisId == 1,]
  model <- EmpiricalCalibration::fitSystematicErrorModel(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr)
  calibratedCis <- calibrateConfidenceInterval(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, model)
  calibratedCis$trueLogRr <- estimatesAnalysis$trueLogRr
  plotTrueAndObserved(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest1_cal.png")
  plotCoverage(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov1_cal.png")
  
  estimatesAnalysis <- estimates[estimates$analysisId == 2,]
  model <- EmpiricalCalibration::fitSystematicErrorModel(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr)
  calibratedCis <- calibrateConfidenceInterval(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, model)
  calibratedCis$trueLogRr <- estimatesAnalysis$trueLogRr
  plotTrueAndObserved(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest2_cal.png")
  plotCoverage(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov2_cal.png")
  
  estimatesAnalysis <- estimates[estimates$analysisId == 3,]
  model <- EmpiricalCalibration::fitSystematicErrorModel(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, estimatesAnalysis$trueLogRr)
  calibratedCis <- calibrateConfidenceInterval(estimatesAnalysis$logRr, estimatesAnalysis$seLogRr, model)
  calibratedCis$trueLogRr <- estimatesAnalysis$trueLogRr
  plotTrueAndObserved(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, "Hazard ratio", fileName = "s:/temp/signalInjectionResults/forest3_cal.png")
  plotCoverage(calibratedCis$logRr, calibratedCis$seLogRr, calibratedCis$trueLogRr, fileName = "s:/temp/signalInjectionResults/cov3_cal.png")
  
}

