#' @keywords internal
.testCode <- function(){
  options("fftempdir" = "s:/temp")
  
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
  cdmDatabaseSchema <- "cdm4_sim.dbo"
  port <- NULL
  
  
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms=dbms, server=server, user=user, password=pw, port=port)
  
  data(omopReferenceSet)
  exposureOutcomePairs <- data.frame(exposureConceptId = 755695, outcomeConceptId = 194133)
  
  createOutcomeCohorts(connectionDetails, cdmDatabaseSchema, createNewCohortTable = TRUE, cohortDatabaseSchema = "scratch.dbo", cohortTable = "mschuemi_omop_hois", referenceSet = "omopReferenceSet")

  mdrr <- computeMdrr(connectionDetails, cdmDatabaseSchema = cdmDatabaseSchema, exposureOutcomePairs = exposureOutcomePairs, outcomeDatabaseSchema = "scratch.dbo", outcomeTable = "mschuemi_omop_hois")
  
  data <- omopReferenceSet
  data$logRr = runif(nrow(data), 0, 1)
  filteredData <- filterOnMdrr(data, mdrr, 1.25)  
  computeAuc(filteredData, omopReferenceSet)
  
  
  x <- injectSignals(connectionDetails, cdmDatabaseSchema = cdmDatabaseSchema, exposureOutcomePairs = exposureOutcomePairs)
}