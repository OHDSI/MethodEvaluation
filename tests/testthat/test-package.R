library(testthat)
library(dplyr)

test_that("Package results", {
  
  # packageOhdsiBenchmarkResults
  
  estimates <- readr::read_delim(col_names = TRUE, delim = ";", trim_ws = TRUE, file = "
    targetId ; outcomeId ; analysisId ; logRr ; seLogRr ; ci95Lb ; ci95Ub
    1 ; 3 ; 1; 0 ; 0 ; 0 ; 0")
  
  controlSummary <- readr::read_delim(col_names = TRUE, delim = ";", trim_ws = TRUE, file = "
    targetId ; comparatorId ; outcomeId ; nestingId; targetName ; comparatorName ; outcomeName ; nestingName ; type ; targetEffectSize ; trueEffectSize ; trueEffectSizeFirstExposure ; oldOutcomeId ; mdrrTarget ; mdrrComparator
      1 ; 2 ; 3 ; 1 ; name1 ; name2 ; name3 ; nest ; fun ; 1.25 ; 1 ; 1 ; 1 ; 1 ; 1") 
  
  analysisRef <- data.frame(method = "CohortMethod",
                            analysisId = 1,
                            description = "1-on-1 matching",
                            details = "",
                            comparative = TRUE,
                            nesting = FALSE,
                            firstExposureOnly = TRUE)
  tempFolder <- tempfile()
  dir.create(tempFolder)
  
  packageOhdsiBenchmarkResults(estimates = estimates,
                               controlSummary = controlSummary,
                               analysisRef = analysisRef,
                               databaseName = "test",
                               exportFolder = tempFolder)
  result <- read.csv(file.path(tempFolder, "estimates_CohortMethod_test.csv"))
  expect_equal(result$analysisId[1], 1)
  
  expect_error(
    metrics <- computeOhdsiBenchmarkMetrics(exportFolder = tempFolder),
    "'response' must have two levels")
  
  unlink(tempFolder, recursive = TRUE)
})
