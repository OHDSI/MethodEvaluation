# Signal injection --------------------------------------------------------
library(MethodEvaluation)
options(andromedaTempFolder = "c:/andromedaTemp")

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = keyring::key_get("pdwServer"),
                                                                port = keyring::key_get("pdwPort"))
oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm_truven_mdcd_v699.dbo"
exposureDatabaseSchema <- cdmDatabaseSchema
outcomeDatabaseSchema <- "scratch.dbo"
outcomeTable <- "mschuemie_outcomes"
nestingCohortDatabaseSchema <- "scratch.dbo"
nestingCohortTable <- "mschuemi_nesting_cohorts"
cdmVersion <- "5"
outputFolder <- "s:/temp/SignalInjectionTemp"

exposureOutcomePairs <- data.frame(exposureId = 1124300, outcomeId = c(24609,
                                                                       29735,
                                                                       73754,
                                                                       80004,
                                                                       134718,
                                                                       139099,
                                                                       141932,
                                                                       192367,
                                                                       193739,
                                                                       194997,
                                                                       197236,
                                                                       199074,
                                                                       255573,
                                                                       257007,
                                                                       313459,
                                                                       314658,
                                                                       316084,
                                                                       319843,
                                                                       321596,
                                                                       374366,
                                                                       375292,
                                                                       380094,
                                                                       433753,
                                                                       433811,
                                                                       436665,
                                                                       436676,
                                                                       436940,
                                                                       437784,
                                                                       438134,
                                                                       440358,
                                                                       440374,
                                                                       443617,
                                                                       443800,
                                                                       4084966,
                                                                       4288310))

covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE,
                                                                useDemographicsAge = TRUE)
ParallelLogger::addDefaultErrorReportLogger(file.path(outputFolder, "ErrorReportR.txt"))
ParallelLogger::addDefaultFileLogger(file.path(outputFolder, "log.txt"))
x <- synthesizePositiveControls(connectionDetails,
                                cdmDatabaseSchema = cdmDatabaseSchema,
                                exposureOutcomePairs = exposureOutcomePairs,
                                exposureDatabaseSchema = cdmDatabaseSchema,
                                exposureTable = "drug_era",
                                outcomeDatabaseSchema = outcomeDatabaseSchema,
                                outcomeTable = outcomeTable,
                                outputDatabaseSchema = outcomeDatabaseSchema,
                                outputTable = outcomeTable,
                                createOutputTable = FALSE,
                                firstExposureOnly = TRUE,
                                firstOutcomeOnly = TRUE,
                                modelType = "survival",
                                prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                                control = createControl(cvType = "auto",
                                                        startingVariance = 0.1,
                                                        noiseLevel = "quiet",
                                                        threads = 10),
                                workFolder = outputFolder,
                                cdmVersion = cdmVersion,
                                covariateSettings = covariateSettings,
                                modelThreads = 3,
                                generationThreads = 1,
                                minOutcomeCountForModel = 100,
                                minOutcomeCountForInjection = 25,
                                washoutPeriod = 183,
                                riskWindowStart = 0,
                                riskWindowEnd = 0,
                                endAnchor = "cohort end",
                                addIntentToTreat = TRUE,
                                removePeopleWithPriorOutcomes = TRUE,
                                maxSubjectsForModel = 1e+05,
                                effectSizes = c(1.5, 2, 4),
                                outputIdOffset = 1000)
saveRDS(x, file.path(outputFolder, "injectionSummary.rds"))


exposureDatabaseSchema <- cdmDatabaseSchema
exposureTable <- "drug_era"
outcomeDatabaseSchema <- outcomeDatabaseSchema
outcomeTable <- outcomeTable
outputDatabaseSchema <- outcomeDatabaseSchema
outputTable <- "mschuemi_test_injection"
createOutputTable <- TRUE
firstExposureOnly <- TRUE
modelType <- "survival"
washoutPeriod <- 183
riskWindowStart <- 0
riskWindowEnd <- 0
addExposureDaysToEnd <- TRUE
firstOutcomeOnly <- FALSE
addIntentToTreat <- TRUE
removePeopleWithPriorOutcomes <- TRUE
effectSizes <- c(1.5, 2, 4)
oracleTempSchema <- NULL
outputIdOffset <- 1000
precision <- 0.01
prior <- createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
control <- createControl(cvType = "auto",
                         startingVariance = 0.1,
                         noiseLevel = "quiet",
                         threads = 10)
workFolder <- "s:/temp/SignalInjectionTemp"
buildModelPerExposure <- FALSE
modelThreads <- 1
generationThreads <- 1
minOutcomeCountForModel <- 100
minOutcomeCountForInjection <- 25
maxSubjectsForModel <- 1e+05


# Run CohortMethod ---------------------------------------------------------
library(CohortMethod)
covariateSettings <- createDefaultCovariateSettings(addDescendantsToExclude = TRUE)

getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(covariateSettings = covariateSettings,
                                                                 firstExposureOnly = TRUE,
                                                                 washoutPeriod = 183)

createStudyPopArgsOnTreatment <- CohortMethod::createCreateStudyPopulationArgs(removeDuplicateSubjects = "keep first",
                                                                               removeSubjectsWithPriorOutcome = TRUE,
                                                                               riskWindowStart = 0,
                                                                               riskWindowEnd = 0,
                                                                               addExposureDaysToEnd = TRUE,
                                                                               minDaysAtRisk = 1)

createStudyPopArgsItt <- CohortMethod::createCreateStudyPopulationArgs(removeDuplicateSubjects = "keep first",
                                                                       removeSubjectsWithPriorOutcome = TRUE,
                                                                       riskWindowStart = 0,
                                                                       riskWindowEnd = 9999,
                                                                       addExposureDaysToEnd = FALSE,
                                                                       minDaysAtRisk = 1)

createPsArgs <- CohortMethod::createCreatePsArgs(control = Cyclops::createControl(noiseLevel = "silent",
                                                                                  cvType = "auto",
                                                                                  tolerance = 2e-07,
                                                                                  cvRepetitions = 1,
                                                                                  startingVariance = 0.01,
                                                                                  seed = 123), maxCohortSizeForFitting = 1e+05)

stratifyByPsArgs <- CohortMethod::createStratifyByPsArgs(numberOfStrata = 10, baseSelection = "all")

fitOutcomeModelArgs1 <- CohortMethod::createFitOutcomeModelArgs(stratified = TRUE,
                                                                modelType = "cox")

cmAnalysis1 <- CohortMethod::createCmAnalysis(analysisId = 1,
                                              description = "PS stratification, on-treatment",
                                              getDbCohortMethodDataArgs = getDbCmDataArgs,
                                              createStudyPopArgs = createStudyPopArgsOnTreatment,
                                              createPs = TRUE,
                                              createPsArgs = createPsArgs,
                                              stratifyByPs = TRUE,
                                              stratifyByPsArgs = stratifyByPsArgs,
                                              fitOutcomeModel = TRUE,
                                              fitOutcomeModelArgs = fitOutcomeModelArgs1)

cmAnalysis2 <- CohortMethod::createCmAnalysis(analysisId = 2,
                                              description = "PS stratification, intent-to-treat",
                                              getDbCohortMethodDataArgs = getDbCmDataArgs,
                                              createStudyPopArgs = createStudyPopArgsItt,
                                              createPs = TRUE,
                                              createPsArgs = createPsArgs,
                                              stratifyByPs = TRUE,
                                              stratifyByPsArgs = stratifyByPsArgs,
                                              fitOutcomeModel = TRUE,
                                              fitOutcomeModelArgs = fitOutcomeModelArgs1)

cmAnalysisList <- list(cmAnalysis1, cmAnalysis2)

x <- readRDS(file.path(outputFolder, "injectionSummary.rds"))

tcos <- createTargetComparatorOutcomes(targetId = 1124300,
                                       comparatorId = 1118084,
                                       outcomeIds = c(x$newOutcomeId,
                                                      192671,
                                                      24609,
                                                      29735,
                                                      73754,
                                                      80004,
                                                      134718,
                                                      139099,
                                                      141932,
                                                      192367,
                                                      193739,
                                                      194997,
                                                      197236,
                                                      199074,
                                                      255573,
                                                      257007,
                                                      313459,
                                                      314658,
                                                      316084,
                                                      319843,
                                                      321596,
                                                      374366,
                                                      375292,
                                                      380094,
                                                      433753,
                                                      433811,
                                                      436665,
                                                      436676,
                                                      436940,
                                                      437784,
                                                      438134,
                                                      440358,
                                                      440374,
                                                      443617,
                                                      443800,
                                                      4084966,
                                                      4288310), excludedCovariateConceptIds = 21603933)
targetComparatorOutcomesList <- list(tcos)

result <- runCmAnalyses(connectionDetails = connectionDetails,
                        cdmDatabaseSchema = cdmDatabaseSchema,
                        exposureDatabaseSchema = cdmDatabaseSchema,
                        exposureTable = "drug_era",
                        outcomeDatabaseSchema = outcomeDatabaseSchema,
                        outcomeTable = outcomeTable,
                        outputFolder = outputFolder,
                        cdmVersion = cdmVersion,
                        cmAnalysisList = cmAnalysisList,
                        targetComparatorOutcomesList = targetComparatorOutcomesList,
                        refitPsForEveryOutcome = FALSE,
                        refitPsForEveryStudyPopulation = FALSE,
                        getDbCohortMethodDataThreads = 1,
                        createPsThreads = 1,
                        psCvThreads = 16,
                        createStudyPopThreads = 3,
                        trimMatchStratifyThreads = 5,
                        prefilterCovariatesThreads = 3,
                        fitOutcomeModelThreads = 5,
                        outcomeCvThreads = 10,
                        outcomeIdsOfInterest = c(192671))
# result <- readRDS(file.path(outputFolder, 'outcomeModelReference.rds'))

analysisSum <- summarizeAnalyses(result)

m <- merge(analysisSum, data.frame(outcomeId = x$newOutcomeId,
                                   targetEffectSize = x$targetEffectSize), all.x = TRUE)
m$targetEffectSize[is.na(m$targetEffectSize)] <- 1
m <- m[m$outcomeId != 192671, ]
m1 <- m[m$analysisId == 1, ]
EmpiricalCalibration::plotCiCalibrationEffect(logRr = m1$logRr,
                                              seLogRr = m1$seLogRr,
                                              trueLogRr = log(m1$targetEffectSize))

m2 <- m[m$analysisId == 2, ]
EmpiricalCalibration::plotCiCalibrationEffect(logRr = m2$logRr,
                                              seLogRr = m2$seLogRr,
                                              trueLogRr = log(m1$targetEffectSize),
                                              fileName = file.path(outputFolder, "ITT.png"))

analysisSum[analysisSum$analysisId == 2 & analysisSum$outcomeId == 24609, ]
x[x$outcomeId == 24609, ]
analysisSum[analysisSum$analysisId == 2 & analysisSum$outcomeId == 1002, ]

# MDRR --------------------------------------------------------------------
library(MethodEvaluation)

pw <- Sys.getenv("pwPostgres")
dbms <- "postgresql"
user <- "postgres"
server <- "localhost/ohdsi"
cdmDatabaseSchema <- "cdm_synpuf"
scratchDatabaseSchema <- "scratch"
outputTable <- "mschuemi_injected_signals"
port <- NULL


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

data("omopReferenceSet")
# exposureOutcomePairs <- data.frame(exposureId = 755695, outcomeId = 194133)

createReferenceSetCohorts(connectionDetails,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          outcomeDatabaseSchema = scratchDatabaseSchema,
                          outcomeTable = "mschuemi_omop_hois",
                          referenceSet = "omopReferenceSet")

mdrr <- computeMdrr(connectionDetails,
                    cdmDatabaseSchema = cdmDatabaseSchema,
                    exposureOutcomePairs = omopReferenceSet,
                    outcomeDatabaseSchema = scratchDatabaseSchema,
                    outcomeTable = "mschuemi_omop_hois")



# Reference set outcome construction --------------------------------------

library(MethodEvaluation)
options(fftempdir = "s:/fftemp")

pw <- Sys.getenv("pwPostgres")
dbms <- "postgresql"
user <- "postgres"
server <- "localhost/ohdsi"
cdmDatabaseSchema <- "cdm_synpuf"
scratchDatabaseSchema <- "scratch"
outputTable <- "mschuemi_injected_signals"
port <- NULL

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

data("omopReferenceSet")

createReferenceSetCohorts(connectionDetails,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          outcomeDatabaseSchema = scratchDatabaseSchema,
                          outcomeTable = "mschuemi_omop_hois",
                          referenceSet = "omopReferenceSet")

data("ohdsiNegativeControls")

createReferenceSetCohorts(connectionDetails,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          outcomeDatabaseSchema = scratchDatabaseSchema,
                          outcomeTable = "mschuemi_ohdsi_hois",
                          nestingDatabaseSchema = scratchDatabaseSchema,
                          nestingTable = "mschuemi_ohdsi_nesting",
                          referenceSet = "ohdsiNegativeControls")


# Run full benchmark ---------------------------------------------------------
library(MethodEvaluation)
options(andromedaTempFolder = "s:/andromedaTemp")
referenceSet <- "ohdsiDevelopment"
maxCores <- parallel::detectCores() - 1

# PDW settings
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "pdw",
                                                                server = keyring::key_get("pdwServer"),
                                                                port = keyring::key_get("pdwPort"))
oracleTempSchema <- NULL
# cdmDatabaseSchema <- "CDM_IBM_CCAE_v1353.dbo"
cdmDatabaseSchema <- "CDM_IBM_MDCD_V1327.dbo"
exposureDatabaseSchema <- "scratch.dbo"
exposureTable <- "mschuemie_cohorts_temp"
outcomeDatabaseSchema <- exposureDatabaseSchema
outcomeTable <- exposureTable
nestingDatabaseSchema <- exposureDatabaseSchema
nestingTable <- exposureTable
cdmVersion <- "5"
outputFolder <- "s:/BenchmarkTest"

# RedShift settings
connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = "redshift",
                                                                connectionString = keyring::key_get("redShiftConnectionStringMdcd"),
                                                                user = keyring::key_get("redShiftUserName"),
                                                                password = keyring::key_get("redShiftPassword"))

Sys.setenv("AWS_OBJECT_KEY" = "bulk")
Sys.setenv("AWS_ACCESS_KEY_ID" = Sys.getenv("bulkUploadS3Key"))
Sys.setenv("AWS_SECRET_ACCESS_KEY" = Sys.getenv("bulkUploadS3Secret"))
Sys.setenv("AWS_BUCKET_NAME" = Sys.getenv("bulkUploadS3Bucket"))
Sys.setenv("AWS_DEFAULT_REGION" = "us-east-1")
Sys.setenv("AWS_SSE_TYPE" = "AES256")
Sys.setenv("USE_MPP_BULK_LOAD" = TRUE)

oracleTempSchema <- NULL
cdmDatabaseSchema <- "cdm"
exposureDatabaseSchema <- "scratch_mschuemi2"
exposureTable <- "mschuemie_cohorts_temp"
outcomeDatabaseSchema <- exposureDatabaseSchema
outcomeTable <- exposureTable
nestingDatabaseSchema <- exposureDatabaseSchema
nestingTable <- exposureTable
cdmVersion <- "5"
outputFolder <- "s:/BenchmarkTest"

# Create cohorts
createReferenceSetCohorts(connectionDetails,
                          oracleTempSchema = oracleTempSchema,
                          cdmDatabaseSchema = cdmDatabaseSchema,
                          exposureDatabaseSchema = exposureDatabaseSchema,
                          exposureTable = exposureTable,
                          outcomeDatabaseSchema = outcomeDatabaseSchema,
                          outcomeTable = outcomeTable,
                          nestingDatabaseSchema = nestingDatabaseSchema,
                          nestingTable = nestingTable,
                          referenceSet = referenceSet,
                          workFolder = outputFolder)

synthesizeReferenceSetPositiveControls(connectionDetails,
                                       oracleTempSchema = oracleTempSchema,
                                       cdmDatabaseSchema = cdmDatabaseSchema,
                                       exposureDatabaseSchema = exposureDatabaseSchema,
                                       exposureTable = exposureTable,
                                       outcomeDatabaseSchema = outcomeDatabaseSchema,
                                       outcomeTable = outcomeTable,
                                       referenceSet = referenceSet,
                                       maxCores = maxCores,
                                       workFolder = outputFolder)

# Run CohortMethod
cmFolder <- file.path(outputFolder, "cohortMethod")
if (!file.exists(cmFolder))
  dir.create(cmFolder)

allControls <- read.csv(file.path(outputFolder , "allControls.csv"))
tcs <- unique(allControls[, c("targetId", "comparatorId", "targetConceptIds", "comparatorConceptIds")])
tcosList <- list()
for (i in 1:nrow(tcs)) {
  outcomeIds <- allControls$outcomeId[allControls$targetId == tcs$targetId[i] &
                                        allControls$comparatorId == tcs$comparatorId[i] &
                                        !is.na(allControls$mdrrComparator)]
  excludedCovariateConceptIds = c(as.numeric(strsplit(tcs$targetConceptIds[i], ";")[[1]]), 
                                  as.numeric(strsplit(tcs$comparatorConceptIds[i], ";")[[1]]))
  if (length(outcomeIds) != 0) {
    tcos <- CohortMethod::createTargetComparatorOutcomes(targetId = tcs$targetId[i],
                                                         comparatorId = tcs$comparatorId[i],
                                                         outcomeIds = outcomeIds,
                                                         excludedCovariateConceptIds = excludedCovariateConceptIds)
    tcosList[[length(tcosList) + 1]] <- tcos
  }
}
covariateSettings <- FeatureExtraction::createDefaultCovariateSettings(addDescendantsToExclude = TRUE)
getDbCmDataArgs <- CohortMethod::createGetDbCohortMethodDataArgs(washoutPeriod = 365,
                                                                 firstExposureOnly = TRUE,
                                                                 removeDuplicateSubjects = "remove all",
                                                                 maxCohortSize = 1e6,
                                                                 covariateSettings = covariateSettings)
createStudyPopArgs <- CohortMethod::createCreateStudyPopulationArgs(removeSubjectsWithPriorOutcome = TRUE,
                                                                    minDaysAtRisk = 1,
                                                                    riskWindowStart = 0,
                                                                    startAnchor = "cohort start",
                                                                    riskWindowEnd = 0,
                                                                    endAnchor = "cohort end")
createPsArgs <- CohortMethod::createCreatePsArgs(errorOnHighCorrelation = TRUE,
                                                 stopOnError = FALSE,
                                                 maxCohortSizeForFitting = 150000,
                                                 control = Cyclops::createControl(cvType = "auto",
                                                                                  startingVariance = 0.01,
                                                                                  noiseLevel = "quiet",
                                                                                  tolerance  = 2e-07,
                                                                                  cvRepetitions = 1))
matchOnPsArgs <- CohortMethod::createMatchOnPsArgs(maxRatio = 1)
fitOutcomeModelArgs <- CohortMethod::createFitOutcomeModelArgs(modelType = "cox", stratified = FALSE)
cmAnalysis <- CohortMethod::createCmAnalysis(analysisId = 1,
                                             description = "1-on-1 matching",
                                             getDbCohortMethodDataArgs = getDbCmDataArgs,
                                             createStudyPopArgs = createStudyPopArgs,
                                             createPs = TRUE,
                                             createPsArgs = createPsArgs,
                                             matchOnPs = TRUE,
                                             matchOnPsArgs = matchOnPsArgs,
                                             fitOutcomeModel = TRUE,
                                             fitOutcomeModelArgs = fitOutcomeModelArgs)
cmAnalysisList <- list(cmAnalysis)
cmResult <- CohortMethod::runCmAnalyses(connectionDetails = connectionDetails,
                                        cdmDatabaseSchema = cdmDatabaseSchema,
                                        oracleTempSchema = oracleTempSchema,
                                        exposureDatabaseSchema = exposureDatabaseSchema,
                                        exposureTable = exposureTable,
                                        outcomeDatabaseSchema = outcomeDatabaseSchema,
                                        outcomeTable = outcomeTable,
                                        outputFolder = cmFolder,
                                        cdmVersion = cdmVersion,
                                        cmAnalysisList = cmAnalysisList,
                                        targetComparatorOutcomesList = tcosList,
                                        refitPsForEveryOutcome = FALSE,
                                        refitPsForEveryStudyPopulation = FALSE,
                                        getDbCohortMethodDataThreads = 1, #min(3, maxCores),
                                        createStudyPopThreads = min(3, maxCores),
                                        createPsThreads = min(3, maxCores),
                                        psCvThreads = min(10, floor(maxCores/3)),
                                        trimMatchStratifyThreads = min(10, maxCores),
                                        fitOutcomeModelThreads = min(max(1, floor(maxCores/8)), 3),
                                        outcomeCvThreads = min(10, maxCores))
cmSummary <- CohortMethod::summarizeAnalyses(cmResult, cmFolder)
saveRDS(cmSummary, file.path(outputFolder, "cmResults.rds"))

# Analyze results
estimates <- readRDS(file.path(outputFolder, "cmResults.rds"))
controlSummary <- read.csv(file.path(outputFolder, "allControls.csv"))
estimates <- data.frame(analysisId = estimates$analysisId,
                        targetId = estimates$targetId,
                        outcomeId = estimates$outcomeId,
                        logRr = estimates$logRr,
                        seLogRr = estimates$seLogRr,
                        ci95Lb = estimates$ci95lb,
                        ci95Ub = estimates$ci95ub)
analysisRef <- data.frame(method = "CohortMethod",
                          analysisId = 1,
                          description = "1-on-1 matching",
                          details = "",
                          comparative = TRUE,
                          nesting = FALSE,
                          firstExposureOnly = TRUE)
exportFolder <- file.path(outputFolder, "export")
estimates <- merge(estimates, 
                   controlSummary[, c("targetId", 
                                      "outcomeId", 
                                      "targetEffectSize", 
                                      "trueEffectSize", 
                                      "trueEffectSizeFirstExposure")])
MethodEvaluation::computeMetrics(logRr = estimates$logRr,
                                 seLogRr = estimates$seLogRr,
                                 ci95Lb = estimates$ci95Lb,
                                 ci95Ub = estimates$ci95Ub,
                                 p = estimates$p,
                                 trueLogRr = log(estimates$trueEffectSizeFirstExposure))
MethodEvaluation::packageOhdsiBenchmarkResults(estimates = estimates,
                                               controlSummary = controlSummary,
                                               analysisRef = analysisRef,
                                               databaseName = "MDCD",
                                               exportFolder = exportFolder,
                                               referenceSet = referenceSet)
MethodEvaluation::launchMethodEvaluationApp(exportFolder = exportFolder)
