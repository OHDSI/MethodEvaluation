library(testthat)

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

test_that("Compute metrics", {
  set.seed(123)
  data <- EmpiricalCalibration::simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
  result <- computeMetrics(logRr = data$logRr, seLogRr = data$seLogRr, trueLogRr = data$trueLogRr)
  expect_equal(length(result), 7)
  # TODO: need to check results (against gold-standard?)
})

test_that("Synthesize positive controls", {
  
  expect_error(synthesizePositiveControls(connectionDetails = connectionDetails,
                                          cdmDatabaseSchema = "main",
                                          exposureOutcomePairs = data.frame(
                                            exposureId = c(1),
                                            outcomeId = c(4))),
               "Not enough negative controls")
  
  # TODO: need to check an actual synthesis
})
