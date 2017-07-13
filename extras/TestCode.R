# Signal injection --------------------------------------------------------
library(MethodEvaluation)
options(fftempdir = "s:/fftemp")

pw <- NULL
dbms <- "pdw"
user <- NULL
server <- "JRDUSAPSCTL01"
cdmDatabaseSchema <- "CDM_Truven_MDCD_V569.dbo"
oracleTempSchema <- NULL
outcomeDatabaseSchema <- "scratch.dbo"
outcomeTable <- "mschuemie_outcomes"
nestingCohortDatabaseSchema <- "scratch.dbo"
nestingCohortTable <- "mschuemi_nesting_cohorts"
port <- 17001
cdmVersion <- "5"

connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

exposureOutcomePairs <- data.frame(exposureId = 1124300,
                                   outcomeId = c(24609, 29735, 73754, 80004, 134718, 139099, 141932, 192367, 193739, 194997, 197236, 199074, 255573, 257007, 313459, 314658, 316084, 319843, 321596, 374366, 375292, 380094, 433753, 433811, 436665, 436676, 436940, 437784, 438134, 440358, 440374, 443617, 443800, 4084966, 4288310))

covariateSettings <- FeatureExtraction::createCovariateSettings(useDemographicsGender = TRUE,
                                                                useDemographicsAge = TRUE,
                                                                useConditionEraLongTerm = TRUE,
                                                                useConditionGroupEraLongTerm = TRUE,
                                                                useDrugEraLongTerm = TRUE,
                                                                useDrugGroupEraLongTerm = TRUE,
                                                                useCharlsonIndex = TRUE)

x <- injectSignals(connectionDetails,
                   cdmDatabaseSchema = cdmDatabaseSchema,
                   exposureOutcomePairs = exposureOutcomePairs,
                   exposureTable = "drug_era",
                   outcomeDatabaseSchema = outcomeDatabaseSchema,
                   outcomeTable = outcomeTable,
                   outputDatabaseSchema = outcomeDatabaseSchema,
                   outputTable = "mschuemi_test_injection",
                   createOutputTable = TRUE,
                   firstExposureOnly = FALSE,
                   firstOutcomeOnly = TRUE,
                   modelType = "survival",
                   prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                   control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10),
                   workFolder = "s:/temp/SignalInjectionTemp",
                   cdmVersion = cdmVersion,
                   covariateSettings = covariateSettings,
                   modelThreads = 4,
                   generationThreads = 3)


exposureDatabaseSchema = cdmDatabaseSchema
exposureTable = "drug_era"
outcomeDatabaseSchema = outcomeDatabaseSchema
outcomeTable = outcomeTable
outputDatabaseSchema = outcomeDatabaseSchema
outputTable = "mschuemi_test_injection"
createOutputTable = TRUE
firstExposureOnly = FALSE
modelType = "poisson"
buildOutcomeModel = TRUE
washoutPeriod = 183
riskWindowStart = 0
riskWindowEnd = 0
addExposureDaysToEnd = TRUE
firstOutcomeOnly = FALSE
effectSizes = c(1, 1.25, 1.5, 2)
oracleTempSchema <- NULL
outputIdOffset = 1000
precision = 0.01
prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10)
workFolder <- "s:/temp/SignalInjectionTemp"
buildModelPerExposure <- FALSE
modelThreads = 1
generationThreads = 1
minOutcomeCount = 100



# MDRR --------------------------------------------------------------------
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

pw <- ""
dbms <- "sql server"
user <- NULL
server <- "RNDUSRDHIT07"
cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
scratchDatabaseSchema <- "scratch.dbo"
outputTable <- "mschuemi_injected_signals"
port <- NULL
cdmVersion <- "4"

pw <- ""
dbms <- "pdw"
user <- NULL
server <- "JRDUSAPSCTL01"
cdmDatabaseSchema <- "cdm_truven_mdcd.dbo"
scratchDatabaseSchema <- "scratch.dbo"
outputTable <- "mschuemi_injected_signals2"
port <- 17001
cdmVersion <- "4"

pw <- ""
dbms <- "pdw"
user <- NULL
server <- "JRDUSAPSCTL01"
cdmDatabaseSchema <- "cdm_truven_mdcd_v446.dbo"
scratchDatabaseSchema <- "scratch.dbo"
outputTable <- "mschuemi_injected_signals"
port <- 17001
cdmVersion <- "5"


connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                server = server,
                                                                user = user,
                                                                password = pw,
                                                                port = port)

data("omopReferenceSet")
#exposureOutcomePairs <- data.frame(exposureId = 755695, outcomeId = 194133)

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


pw <- ""
dbms <- "pdw"
user <- NULL
server <- "JRDUSAPSCTL01"
cdmDatabaseSchema <- "cdm_truven_mdcd_v569.dbo"
scratchDatabaseSchema <- "scratch.dbo"
outputTable <- "mschuemi_injected_signals"
port <- 17001
cdmVersion <- "5"


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
