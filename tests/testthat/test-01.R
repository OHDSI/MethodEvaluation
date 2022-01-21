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
    suppressWarnings(
      synthesizePositiveControls(connectionDetails = connectionDetails,
                                 cdmDatabaseSchema = "main",
                                 exposureOutcomePairs = data.frame(
                                   exposureId = c(1),
                                   outcomeId = c(4)))
    ),
    "Not enough negative controls")
  
  # TODO: need to check an actual synthesis
  
  expect_error(
    suppressWarnings( 
      injectSignals(connectionDetails = connectionDetails,
                    cdmDatabaseSchema = "main",
                    exposureOutcomePairs = data.frame(
                      exposureId = c(1),
                      outcomeId = c(4)))
    ),
    "Not enough negative controls")
})

test_that("Create reference set cohort", {
  tempFolder <- tempfile()
  dir.create(tempFolder)
  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "omopReferenceSet",
                            workFolder = tempFolder)
  
  # TODO Need a check for completion
  
  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "ohdsiMethodsBenchmark",
                            workFolder = tempFolder)
  cohortCounts <- read.csv(file.path(tempFolder, "cohortCounts.csv"))
  controls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                  package = "MethodEvaluation")) %>%
    filter(.data$outcomeId > 4) %>%
    distinct(cohortId = .data$outcomeId) 
  expect_equal(
    nrow(cohortCounts %>% filter(type == "Outcome")),
    nrow(controls) + 4) # adding c("acute_pancreatitis", "gi_bleed", "stroke", "ibd")
  
  createReferenceSetCohorts(connectionDetails = connectionDetails,
                            cdmDatabaseSchema = "main",
                            referenceSet = "ohdsiDevelopment",
                            workFolder = tempFolder)
  cohortCounts <- read.csv(file.path(tempFolder, "cohortCounts.csv"))
  controls <- readRDS(system.file("ohdsiDevelopmentNegativeControls.rds",
                                  package = "MethodEvaluation"))
  expect_equal(
    nrow(cohortCounts %>% filter(type == "Outcome")),
    nrow(controls))
  unlink(tempFolder, recursive = TRUE)
})


test_that("Synthesize positive control reference set cohort", {
  tempFolder <- tempfile()
  dir.create(tempFolder)
  expect_error(
    suppressWarnings(
      synthesizeReferenceSetPositiveControls(connectionDetails =  connectionDetails,
                                             cdmDatabaseSchema = "main", 
                                             referenceSet = "ohdsiMethodsBenchmark",
                                             workFolder = tempFolder)
    ),
    "Not enough negative controls")
  expect_error(
    suppressWarnings(
      synthesizeReferenceSetPositiveControls(connectionDetails =  connectionDetails,
                                             cdmDatabaseSchema = "main", 
                                             referenceSet = "ohdsiDevelopment",
                                             workFolder = tempFolder)
    ),
    "Not enough negative controls")
  unlink(tempFolder, recursive = TRUE)
})
