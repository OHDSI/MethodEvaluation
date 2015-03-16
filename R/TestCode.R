#' @keywords internal
.testCode <- function(){
  options("fftempdir" = "s:/temp")
  
  pw <- pw
  dbms <- "postgresql"
  user <- "postgres"
  server <- "localhost/ohdsi"
  cdmDatabaseSchema <- "cdm_truven_ccae_6k"
  port <- NULL
  
  connectionDetails <- DatabaseConnector::createConnectionDetails(dbms=dbms, server=server, user=user, password=pw, port=port)
  
  exposureOutcomePairs <- data.frame(exposureConceptId = 755695, outcomeConceptId = 194133)
  mdrr <- computeMdrr(connectionDetails, cdmDatabaseSchema = cdmDatabaseSchema, exposureOutcomePairs = exposureOutcomePairs)
  
  x <- injectSignals(connectionDetails, cdmDatabaseSchema = cdmDatabaseSchema, exposureOutcomePairs = exposureOutcomePairs)
}