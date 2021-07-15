library(testthat)
library(dplyr)

connectionDetails <- Eunomia::getEunomiaConnectionDetails()
Eunomia::createCohorts(connectionDetails)

test_that("Compute MDRR", {
  result <- computeMdrr(connectionDetails = connectionDetails,
                        cdmDatabaseSchema = "main",
                        exposureOutcomePairs = data.frame(
                          exposureId = c(1,2),
                          outcomeId = c(4,5)),
                        exposureTable = "cohort",
                        outcomeTable = "cohort")
  
  expect_equal(ncol(result), 7)
  # TODO: need to check results (that are currently empty)
})

test_that("Synthesize positive controls", {
  
  expect_error(
    synthesizePositiveControls(connectionDetails = connectionDetails,
                               cdmDatabaseSchema = "main",
                               exposureOutcomePairs = data.frame(
                                 exposureId = c(1),
                                 outcomeId = c(4))),
    "Not enough negative controls")
  
  # TODO: need to check an actual synthesis
})

test_that("Create reference set cohort", {

  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "omopReferenceSet",
                            workFolder = tempDir)
  
  # TODO Need a check for completion
  
  tempDir <- tempdir()  
  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "ohdsiMethodsBenchmark",
                            workFolder = tempDir)
  cohortCounts <- read.csv(file.path(tempDir, "cohortCounts.csv"))
  controls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                  package = "MethodEvaluation")) %>%
    filter(.data$outcomeId > 4) %>%
    distinct(cohortId = .data$outcomeId) 
  expect_equal(
    nrow(cohortCounts %>% filter(type == "Outcome")),
    nrow(controls) + 4) # adding c("acute_pancreatitis", "gi_bleed", "stroke", "ibd")
  unlink(tempDir, recursive = TRUE)
  
  tempDir <- tempdir()  
  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "ohdsiDevelopment",
                            workFolder = tempDir)
  cohortCounts <- read.csv(file.path(tempDir, "cohortCounts.csv"))
  controls <- readRDS(system.file("ohdsiDevelopmentNegativeControls.rds",
                                  package = "MethodEvaluation"))
  expect_equal(
    nrow(cohortCounts %>% filter(type == "Outcome")),
    nrow(controls))
  unlink(tempDir, recursive = TRUE)
})


test_that("Synthesize positive control reference set cohort", {
  tempDir <- tempdir()
  expect_error(
    synthesizeReferenceSetPositiveControls(connectionDetails =  connectionDetails,
                                           cdmDatabaseSchema = "main", 
                                           referenceSet = "ohdsiMethodsBenchmark",
                                           workFolder = tempDir),
    "Not enough negative controls")
  unlink(tempDir, recursive = TRUE)
  
  tempDir <- tempdir()
  expect_error(
    synthesizeReferenceSetPositiveControls(connectionDetails =  connectionDetails,
                                           cdmDatabaseSchema = "main", 
                                           referenceSet = "ohdsiDevelopment",
                                           workFolder = tempDir),
    "Not enough negative controls")
  unlink(tempDir, recursive = TRUE)
})