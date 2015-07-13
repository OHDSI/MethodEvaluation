#' @keywords internal
.testCode <- function() {
  library(MethodEvaluation)
  options(fftempdir = "s:/temp")

  pw <- pw
  dbms <- "postgresql"
  user <- "postgres"
  server <- "localhost/ohdsi"
  cdmDatabaseSchema <- "cdm_truven_ccae_6k"
  port <- NULL

  pw <- ""
  dbms <- "sql server"
  user <- NULL
  server <- "RNDUSRDHIT07"
  cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
  scratchDatabaseSchema <- "scratch.dbo"
  outputTable <- "mschuemi_injected_signals"
  port <- NULL

  
  pw <- ""
  dbms <- "pdw"
  user <- NULL
  server <- "JRDUSAPSCTL01"
  cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
  scratchDatabaseSchema <- "scratch.dbo"
  outputTable <- "mschuemi_injected_signals"
  port <- 17001
  

  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  server = server,
                                                                  user = user,
                                                                  password = pw,
                                                                  port = port)

  data("omopReferenceSet")
  #exposureOutcomePairs <- data.frame(exposureConceptId = 755695, outcomeConceptId = 194133)

  createOutcomeCohorts(connectionDetails,
                       cdmDatabaseSchema,
                       createNewCohortTable = TRUE,
                       cohortDatabaseSchema = scratchDatabaseSchema,
                       cohortTable = "mschuemi_omop_hois",
                       referenceSet = "omopReferenceSet")

  mdrr <- computeMdrr(connectionDetails,
                      cdmDatabaseSchema = cdmDatabaseSchema,
                      exposureOutcomePairs = omopReferenceSet,
                      outcomeDatabaseSchema = scratchDatabaseSchema,
                      outcomeTable = "mschuemi_omop_hois")
  
  
  negControls <- mdrr[mdrr$groundTruth == 0 & mdrr$mdrr < 1.25,]
  nrow(negControls)
  saveRDS(negControls, "s:/temp/negControls.rds")
  negControls <- readRDS("s:/temp/negControls.rds")  
  
  covariateSettings <- PatientLevelPrediction::createCovariateSettings(useCovariateDemographics = TRUE,
                                                                       useCovariateConditionOccurrence = TRUE,
                                                                       useCovariateConditionOccurrence365d = FALSE,
                                                                       useCovariateConditionOccurrence30d = FALSE,
                                                                       useCovariateConditionOccurrenceInpt180d = FALSE,
                                                                       useCovariateConditionEra = FALSE,
                                                                       useCovariateConditionEraEver = FALSE,
                                                                       useCovariateConditionEraOverlap = FALSE,
                                                                       useCovariateConditionGroup = FALSE,
                                                                       useCovariateDrugExposure = FALSE,
                                                                       useCovariateDrugExposure365d = FALSE,
                                                                       useCovariateDrugExposure30d = FALSE,
                                                                       useCovariateDrugEra = FALSE,
                                                                       useCovariateDrugEra365d = FALSE,
                                                                       useCovariateDrugEra30d = FALSE,
                                                                       useCovariateDrugEraEver = FALSE,
                                                                       useCovariateDrugEraOverlap = FALSE,
                                                                       useCovariateDrugGroup = FALSE,
                                                                       useCovariateProcedureOccurrence = FALSE,
                                                                       useCovariateProcedureOccurrence365d = FALSE,
                                                                       useCovariateProcedureOccurrence30d = FALSE,
                                                                       useCovariateProcedureGroup = FALSE,
                                                                       useCovariateObservation = FALSE,
                                                                       useCovariateObservation365d = FALSE,
                                                                       useCovariateObservation30d = FALSE,
                                                                       useCovariateObservationCount365d = FALSE,
                                                                       useCovariateMeasurement365d = FALSE,
                                                                       useCovariateMeasurement30d = FALSE,
                                                                       useCovariateMeasurementCount365d = FALSE,
                                                                       useCovariateMeasurementBelow = FALSE,
                                                                       useCovariateMeasurementAbove = FALSE,
                                                                       useCovariateConceptCounts = FALSE,
                                                                       useCovariateRiskScores = FALSE,
                                                                       useCovariateInteractionYear = FALSE,
                                                                       useCovariateInteractionMonth = FALSE,
                                                                       excludedCovariateConceptIds = c(),
                                                                       deleteCovariatesSmallCount = 100)
  
  
  x <- injectSignals(connectionDetails,
                     cdmDatabaseSchema = cdmDatabaseSchema,
                     exposureOutcomePairs = negControls[1,],
                     exposureTable = "drug_era",
                     outcomeDatabaseSchema = scratchDatabaseSchema,
                     outcomeTable = "mschuemi_omop_hois",
                     outputDatabaseSchema = scratchDatabaseSchema,
                     outputTable = outputTable,
                     createOutputTable = TRUE,
                     firstExposureOnly = FALSE,
                     modelType = "survival",
                     covariateSettings = covariateSettings)
  
  exposureOutcomePairs = negControls[1,]
  exposureDatabaseSchema = cdmDatabaseSchema
  exposureTable = "drug_era"
  outcomeDatabaseSchema = scratchDatabaseSchema
  outcomeTable = "mschuemi_omop_hois"
  outcomeConditionTypeConceptIds = c()
  outputDatabaseSchema = scratchDatabaseSchema
  outputTable = outputTable
  createOutputTable = TRUE
  firstExposureOnly = FALSE
  modelType = "survival"
  buildOutcomeModel = TRUE
  washoutWindow = 183
  riskWindowStart = 0
  riskWindowEnd = 0
  addExposureDaysToEnd = TRUE
  firstOutcomeOnly = FALSE
  effectSizes = c(1, 1.25, 1.5, 2, 4, 8)
  oracleTempSchema <- NULL
  outputConceptIdOffset = 1000
  
  
}
  





