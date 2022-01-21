library(testthat)

test_that("Plot ROCs", {
  set.seed(123)
  data <- EmpiricalCalibration::simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
  fileName <- tempfile(pattern = "roc", fileext = ".png")
  
  plot <- plotRocsInjectedSignals(logRr = data$logRr, trueLogRr = data$trueLogRr, showAucs = TRUE,
                                  fileName = fileName)
  
  expect_false(is.null(plot))
  expect_true(file.exists(fileName))
  
  unlink(fileName)
})

test_that("Plot coverage", {
  set.seed(123)
  data <- EmpiricalCalibration::simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
  fileName <- tempfile(pattern = "cov", fileext = ".png")
  
  plot <- plotCoverageInjectedSignals(logRr = data$logRr, seLogRr = data$seLogRr, trueLogRr = data$trueLogRr,
                                  fileName = fileName)
  
  expect_false(is.null(plot))
  expect_true(file.exists(fileName))
  
  unlink(fileName)
})

test_that("Plot controls", {
  set.seed(123)
  data <- EmpiricalCalibration::simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
  fileName <- tempfile(pattern = "con", fileext = ".png")
  
  plot <- plotControls(logRr = data$logRr, seLogRr = data$seLogRr, trueLogRr = data$trueLogRr,
                                      fileName = fileName)
  
  expect_false(is.null(plot))
  expect_true(file.exists(fileName))
  
  unlink(fileName)
})
