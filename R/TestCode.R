#' @keywords internal
.testCode <- function() {
  library(MethodEvaluation)
  options(fftempdir = "s:/temp")
  
  pw <- pw
  dbms <- "postgresql"
  user <- "postgres"
  server <- "localhost/ohdsi"
  cdmDatabaseSchema <- "cdm_truven_ccae_6k"
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
                     firstOutcomeOnly = TRUE,
                     modelType = "survival"
                     ,covariateSettings = covariateSettings
  )
  
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
  effectSizes = c(1, 1.25, 1.5, 2, 4)
  oracleTempSchema <- NULL
  outputConceptIdOffset = 1000
  precision = 0.01
  prior = createPrior("laplace", exclude = 0, useCrossValidation = TRUE)
  control = createControl(cvType = "auto", startingVariance = 0.1, noiseLevel = "quiet", threads = 10)
  
  
  
  # Test PDW performance issues:
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms = dbms,
                                                                  server = server,
                                                                  user = user,
                                                                  password = pw,
                                                                  port = port,
                                                                  schema = cdmDatabaseSchema)
  conn <- connect(connectionDetails)
  data <- querySql(conn, "SELECT TOP 100000 * FROM person")
  data <- data[,c("PERSON_ID","GENDER_CONCEPT_ID","YEAR_OF_BIRTH","RACE_CONCEPT_ID","ETHNICITY_CONCEPT_ID")]
  system.time(
    insertTable(conn, "#temp", data, TRUE, TRUE, TRUE)
  )
  #user  system elapsed 
  #0.47    0.00   22.55 
  
  # Using CTAS hack:
  #user  system elapsed 
  #0.29    0.01    2.83 
  
  names(data)[names(data) == "PERSON_ID"] <- "ID"
  system.time(
    insertTable(conn, "#temp", data, TRUE, TRUE, TRUE)
  )
  #user  system elapsed 
  #0.46    0.00   24.83 
  
  
  tableName = "#temp"
  connection <- conn
  
  .sql.qescape <- function(s, identifier=FALSE, quote="\"") {
    s <- as.character(s)
    if (identifier) {
      vid <- grep("^[A-Za-z]+([A-Za-z0-9_]*)$",s)
      if (length(s[-vid])) {
        if (is.na(quote)) stop("The JDBC connection doesn't support quoted identifiers, but table/column name contains characters that must be quoted (",paste(s[-vid],collapse=','),")")
        s[-vid] <- .sql.qescape(s[-vid], FALSE, quote)
      }
      return(s)
    }
    if (is.na(quote)) quote <- ''
    s <- gsub("\\\\","\\\\\\\\",s)
    if (nchar(quote)) s <- gsub(paste("\\",quote,sep=''),paste("\\\\\\",quote,sep=''),s,perl=TRUE)
    paste(quote,s,quote,sep='')
  }
  def = function(obj) {
    if (is.integer(obj)) "INTEGER"
    else if (is.numeric(obj)) "FLOAT"
    else "VARCHAR(255)"
  }
  fts <- sapply(data, def)
  fdef <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote),fts,collapse=',')
  qname <- .sql.qescape(tableName, TRUE, connection@identifier.quote)

  esc <- function(str){
    paste("'",gsub("'","''",str),"'",sep="")
  }
  varNames <- paste(.sql.qescape(names(data), TRUE, connection@identifier.quote),collapse=',')
  
  start = 1
  end = nrow(data)
  valueString <- paste(apply(sapply(data[start:end,],esc),MARGIN=1,FUN = paste,collapse=","),collapse="\nUNION ALL\nSELECT ")
  sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ", qname, " (",varNames," ) WITH (LOCATION = USER_DB, DISTRIBUTION=REPLICATE) AS SELECT ",valueString,sep= '')
  system.time(
    executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  )
  
  
  valueString <- paste(apply(sapply(data[start:end,],esc),MARGIN=1,FUN = paste,collapse=","),collapse="\nUNION ALL\nSELECT ")
  sql <- paste("IF XACT_STATE() = 1 COMMIT; CREATE TABLE ", qname, " (",varNames," ) WITH (LOCATION = USER_DB, DISTRIBUTION=REPLICATE) AS SELECT ",valueString,sep= '')
  executeSql(connection, sql, progressBar = FALSE, reportOverallTime = FALSE)
  
  sql <- "IF XACT_STATE() = 1 COMMIT; CREATE TABLE #temp (a,b) WITH (LOCATION = USER_DB, DISTRIBUTION=REPLICATE) AS SELECT CAST('1' AS INT),'b' UNION ALL SELECT '2', 'd'"
  executeSql(conn, sql, progressBar = FALSE, reportOverallTime = FALSE)
  x <- querySql(conn,"select * from #temp")
  executeSql(connection, "DROP TABLE #temp", progressBar = FALSE, reportOverallTime = FALSE)
    
  
  sql <- "select n from (values (1),(2),(3),(4),(5),(6),(7),(8),(9),(10)) t(n)"
  querySql(conn, sql)
  
  dbDisconnect(conn)  
}






