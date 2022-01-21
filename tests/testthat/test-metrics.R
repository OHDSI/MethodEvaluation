library(testthat)
library(dplyr)

test_that("Compute metrics", {
  set.seed(123)
  data <- EmpiricalCalibration::simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
  result <- computeMetrics(logRr = data$logRr, seLogRr = data$seLogRr, trueLogRr = data$trueLogRr)
  expect_equal(length(result), 7)
  # TODO: need to check results (against gold-standard?)
})
