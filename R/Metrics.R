# @file Metrics.R
#
# Copyright 2018 Observational Health Data Sciences and Informatics
#
# This file is part of MethodEvaluation
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#' Compute the AUCs for various injected signal sizes
#'
#' @param logRr       A vector containing the log of the relative risk as estimated by a method.
#' @param trueLogRr   A vector containing the injected log(relative risk) for each estimate.
#' 
#' @return
#' A data frame with per injected signal size the AUC and the 95 percent confidence interval of the AUC.
#' 
#' @export
computeAucs <- function(logRr, trueLogRr) {
  if (any(is.na(logRr))) {
    warning("Some estimates are NA, removing prior to computing AUCs")
    trueLogRr <- trueLogRr[!is.na(logRr)]
    logRr <- logRr[!is.na(logRr)]
  }
  trueLogRrLevels <- unique(trueLogRr)
  if (all(trueLogRrLevels != 0))
    stop("Requiring at least one true relative risk of 1")
  result <- data.frame(trueLogRr = trueLogRrLevels[trueLogRrLevels != 0], 
                       auc = 0, 
                       aucLb95Ci = 0, 
                       aucUb95Ci = 0)
  for (i in 1:nrow(result)) {
    data <- data.frame(logRr = logRr[trueLogRr == 0 | trueLogRr == result$trueLogRr[i]], 
                       trueLogRr = trueLogRr[trueLogRr == 0 | trueLogRr == result$trueLogRr[i]])
    data$truth <- data$trueLogRr != 0
    roc <- pROC::roc(data$truth, data$logRr, algorithm = 3)
    auc <- pROC::ci.auc(roc, method = "delong")
    result$auc[i] <- auc[1]
    result$aucLb95Ci[i] <- auc[2]
    result$aucUb95Ci[i] <- auc[3]
  }
  return(result)
}

#' Compute the coverage
#'
#' @details
#' Compute the fractions of estimates where the true effect size is below, above or within the confidence
#' interval, for one or more true effect sizes.
#'
#' @param logRr       A numeric vector of effect estimates on the log scale.
#' @param seLogRr     The standard error of the log of the effect estimates. Hint: often the standard
#'                    error = (log(<lower bound 95 percent confidence interval>) - log(<effect
#'                    estimate>))/qnorm(0.025).
#' @param trueLogRr   A vector of the true effect sizes.
#' @param region      Size of the confidence interval. Default is .95 (95 percent).
#'
#' @export
computeCoverage <- function(logRr, seLogRr, trueLogRr, region = 0.95) {
  data <- data.frame(logRr = logRr,
                     logLb95Rr = logRr + qnorm((1 - region)/2) * seLogRr,
                     logUb95Rr = logRr + qnorm(1 - (1 - region)/2) * seLogRr,
                     trueLogRr = trueLogRr)
  if (any(is.na(data$logRr))) {
    warning("Some estimates are NA, removing prior to computing coverage")
    data <- data[!is.na(data$logRr), ]
  }
  data$aboveCi <- (data$trueLogRr > data$logUb95Rr)
  data$withCi <- (data$trueLogRr >= data$logLb95Rr & data$logRr <= data$logUb95Rr)
  data$belowCi <- (data$trueLogRr < data$logLb95Rr)
  result <- aggregate(aboveCi ~ trueLogRr, data = data, mean)
  result <- merge(result, aggregate(withCi ~ trueLogRr, data = data, mean))
  result <- merge(result, aggregate(belowCi ~ trueLogRr, data = data, mean))
  return(result)
}

#' Compute the mean squared error
#' 
#' @param logRr       A numeric vector of effect estimates on the log scale.
#' @param trueLogRr   A vector of the true effect sizes.
#'
#' @export
computeMse <- function(logRr, trueLogRr) {
  if (any(is.na(logRr))) {
    warning("Some estimates are NA, removing prior to computing coverage")
    trueLogRr <- trueLogRr[!is.na(logRr)]
    logRr <- logRr[!is.na(logRr)]
  }

  trueLogRrLevels <- unique(trueLogRr)
  result <- data.frame(trueLogRr = trueLogRrLevels, 
                       mse = NA)
  for (i in 1:nrow(result)) {
    target <- result$trueLogRr[i]
    result$mse[i] <- mean((logRr[trueLogRr == target] - target) ^ 2)
  }
  return(result)
}

#' Compute type 1 and 2 error
#' 
#' @param logRr       A numeric vector of effect estimates on the log scale.
#' @param seLogRr     The standard error of the log of the effect estimates. Hint: often the standard
#'                    error = (log(<lower bound 95 percent confidence interval>) - log(<effect
#'                    estimate>))/qnorm(0.025).
#' @param trueLogRr   A vector of the true effect sizes.
#' @param alpha       The alpha (expected type I error).
#'
#' @export
computeType1And2Error <- function(logRr, seLogRr, trueLogRr, alpha = 0.05) {
  if (any(is.na(seLogRr))) {
    warning("Some estimates are NA, removing prior to computing coverage")
    trueLogRr <- trueLogRr[!is.na(seLogRr)]
    logRr <- logRr[!is.na(seLogRr)]
    seLogRr <- seLogRr[!is.na(seLogRr)]
  }
  
  trueLogRrLevels <- unique(trueLogRr)
  result <- data.frame(trueLogRr = trueLogRrLevels, 
                       type1Error = NA,
                       type2Error = NA)
  for (i in 1:nrow(result)) {
    target <- result$trueLogRr[i]
    computeTraditionalP <- function(logRr, seLogRr) {
      z <- logRr/seLogRr
      return(2 * pmin(pnorm(z), 1 - pnorm(z)))  # 2-sided p-value
    }
    p <- computeTraditionalP(logRr[trueLogRr == target], seLogRr[trueLogRr == target])
    if (target == 0){
        result$type1Error[i] <- mean(p < alpha)
    } else {
      result$type2Error[i] <- mean(p > alpha)
    }
  }
  return(result)
}

#' Compute the AUC, coverage, MSE, and type 1 and 2 error
#'
#' @details
#' Compute the AUC, coverage, MSE, and type 1 and 2 error.
#'
#' @param logRr       A numeric vector of effect estimates on the log scale
#' @param seLogRr     The standard error of the log of the effect estimates. Hint: often the standard
#'                    error = (log(<lower bound 95 percent confidence interval>) - log(<effect
#'                    estimate>))/qnorm(0.025)
#' @param trueLogRr   A vector of the true effect sizes
#'
#' @export
computeMetrics <- function(logRr, seLogRr, trueLogRr) {
  # data <- EmpiricalCalibration::simulateControls(n = 50 * 3, mean = 0.25, sd = 0.25, trueLogRr = log(c(1, 2, 4))); logRr <- data$logRr; seLogRr <- data$seLogRr; trueLogRr <- data$trueLogRr
  aucs <- computeAucs(logRr = logRr, trueLogRr = trueLogRr)
  coverage <- computeCoverage(logRr = logRr, seLogRr = seLogRr, trueLogRr = trueLogRr)
  coverage <- data.frame(trueLogRr = coverage$trueLogRr,
                         coverage = coverage$withCi)
  mse <- computeMse(logRr = logRr, trueLogRr = trueLogRr)
  type1And2Error <- computeType1And2Error(logRr = logRr, seLogRr = seLogRr, trueLogRr = trueLogRr)
  metrics <- merge(aucs, coverage, all.y = TRUE)
  metrics <- merge(metrics, mse)
  metrics <- merge(metrics, type1And2Error)
  return(metrics)
}

checkHasColumns <- function(df, columnNames, argName) {
  if (class(df) != "data.frame") {
    stop("Argument '", argName, "' is not a data.frame")
  }
  for (columnName in columnNames) {
    if (!(columnName %in% colnames(df))) {
      stop("Argument '", argName, "' does not have column '", columnName, "'")
    }
  }
}

#' Package results of a method on the OHDSI Methods Benchmark
#' 
#' @description  
#' Stores the results of a method on the OHDSI Methods Benchmark in a standardized format, for example
#' for use in the Method Evaluation Shiny app.
#'
#' @param estimates        A data frame containing the estimates. See details for required columns.
#' @param controlSummary   A data frame with the summary of the controls as generated by the \code{\link{synthesizePositiveControls}} 
#'                         function.
#' @param analysisRef      A file describing the various analyses that were performed. See details for required columns.
#' @param databaseName     A character string to identify the database the method was executed on.
#' @param exportFolder     The folder where the output CSV files will written.
#' 
#' @details 
#' The \code{estimates} argument should have the following columns: "targetId", "outcomeId", "analysisId", "logRr", "seLogRr", "ci95Lb", "ci95Ub".
#' 
#' The \code{analysisRef} argument should have the following columns: "analysisId", "method", "comparative", "nesting", "firstExposureOnly"
#' 
#' The \code{targetId} and \code{outcomeId} fields identify the specific control, and should correspond to those in the controlSummary object.
#' 
#' The \code{analysisId} field is an integer that identifies a specific variant of the method. For example, if the method is 'CohortMethod', 
#' analysisId = 1 could identify a set of settings using propensity score matching, and analysisId = 2 could identify a set of settings using
#' stratification.
#' 
#' \code{logRr}, \code{seLogRr}, \code{ci95Lb}, and \code{ci95Ub} correspond to the log of the effect estimate (e.g. the hazard ratio), the standard error, and the upper
#' and lower bound of the effect size estimate, as produced by the method.
#' 
#' \code{method} is a character string identifyign the method (e.g. "CohortMethod").
#' 
#' \code{comparative} is a boolean indicating whether the analysis can also be considerd to perform comparative effect estimation (comparing the target 
#' to the comparator).
#' 
#' \code{nesting} is a boolean indicating whether the  analysis is nested in the nesting cohorts identified in the gold standard.
#' 
#' \code{firstExposureOnly} is a boolean indicating whether only the first exposure was used in the analysis.
#' 
#' @export
packageOhdsiBenchmarkResults <- function(estimates, 
                                         controlSummary, 
                                         analysisRef, 
                                         databaseName,
                                         exportFolder) {
  checkHasColumns(estimates, c("targetId", "outcomeId", "analysisId", "logRr", "seLogRr", "ci95Lb", "ci95Ub"), "estimates")
  checkHasColumns(controlSummary, c("outcomeId", "comparatorId", "targetId", "targetName", "comparatorName", "nestingId", 
                                    "nestingName", "outcomeName", "type", "targetEffectSize", "trueEffectSize", "trueEffectSizeFirstExposure", 
                                    "oldOutcomeId", "mdrrTarget", "mdrrComparator"), "controlSummary")
  checkHasColumns(analysisRef, c("analysisId", "method", "comparative", "nesting", "firstExposureOnly"), "analysisRef")
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  
  # Create full grid of controls (including those that did not make it in the database:
  ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds", package = "MethodEvaluation"))
  ohdsiNegativeControls$oldOutcomeId <- ohdsiNegativeControls$outcomeId
  ohdsiNegativeControls$stratum <- ohdsiNegativeControls$outcomeName
  idx <- ohdsiNegativeControls$type == "Outcome control"
  ohdsiNegativeControls$stratum[idx] <- ohdsiNegativeControls$targetName[idx]
  ohdsiNegativeControls <- ohdsiNegativeControls[, c("targetId", "targetName", "comparatorId", "comparatorName", "nestingId", "nestingName", "oldOutcomeId", "outcomeName", "type", "stratum")]
  fullGrid <- do.call("rbind", replicate(4, ohdsiNegativeControls, simplify = FALSE))
  fullGrid$targetEffectSize <- rep(c(1, 1.5, 2, 4), each = nrow(ohdsiNegativeControls))
  idx <- fullGrid$targetEffectSize != 1
  fullGrid$outcomeName[idx] <- paste0(fullGrid$outcomeName[idx], ", RR=", fullGrid$targetEffectSize[idx])
  allControls <- merge(controlSummary, fullGrid, all.y = TRUE)

  # Merge estimates into full grid:
  analysisIds <- unique(analysisRef$analysisId)
  fullGrid <- do.call("rbind", replicate(length(analysisIds), allControls, simplify = FALSE))
  fullGrid$analysisId <- rep(analysisIds, each = nrow(allControls))
  estimates <- merge(fullGrid, estimates[, c("targetId", "outcomeId", "analysisId", "logRr", "seLogRr", "ci95Lb", "ci95Ub")], all.x = TRUE)
  
  # Add meta-data:
  estimates <- merge(estimates, analysisRef[, c("analysisId", "method", "comparative", "nesting", "firstExposureOnly")])
  estimates$database <- databaseName
  
  # Perform empirical calibration:
  combis <- unique(estimates[, c("method", "analysisId", "stratum")])
  calibrate <- function(i , combis, estimates) {
    subset <- estimates[estimates$method == combis$method[i] & 
                          estimates$analysisId == combis$analysisId[i] & 
                          estimates$stratum == combis$stratum[i], ]
    subset$rootOutcomeName <- gsub(", RR.*$", "", as.character(subset$outcomeName))
    subset$leaveOutUnit <- paste(subset$targetId, subset$rootOutcomeName)
    filterSubset <- subset[!is.na(subset$seLogRr) & !is.infinite(subset$seLogRr), ]
    if (nrow(filterSubset) < 5 || length(unique(filterSubset$targetEffectSize)) < 2) {
      subset$calLogRr <- rep(NA, nrow(subset))
      subset$calSeLogRr <- rep(NA, nrow(subset))
      subset$calCi95Lb <- rep(NA, nrow(subset))
      subset$calCi95Ub <- rep(NA, nrow(subset))
      subset$calP <- rep(NA, nrow(subset))
    } else {
      # Use leave-one out when calibrating to not overestimate
      
      calibrateLeaveOneOut <- function(leaveOutUnit) {
        subsetMinusOne <- filterSubset[filterSubset$leaveOutUnit != leaveOutUnit, ]
        one <- subset[subset$leaveOutUnit == leaveOutUnit, ]
        model <- EmpiricalCalibration::fitSystematicErrorModel(logRr = subsetMinusOne$logRr,
                                                               seLogRr = subsetMinusOne$seLogRr,
                                                               trueLogRr = log(subsetMinusOne$targetEffectSize),
                                                               estimateCovarianceMatrix = FALSE)
        caliCi <- EmpiricalCalibration::calibrateConfidenceInterval(logRr = one$logRr,
                                                                    seLogRr = one$seLogRr,
                                                                    model = model)
        null <- EmpiricalCalibration::fitNull(logRr = subsetMinusOne$logRr[subsetMinusOne$targetEffectSize == 1],
                                              seLogRr = subsetMinusOne$seLogRr[subsetMinusOne$targetEffectSize == 1])
        caliP <- EmpiricalCalibration::calibrateP(null = null,
                                                  logRr = one$logRr,
                                                  seLogRr = one$seLogRr)
        one$calLogRr <- caliCi$logRr
        one$calSeLogRr <- caliCi$seLogRr
        one$calCi95Lb <- exp(caliCi$logLb95Rr)
        one$calCi95Ub <- exp(caliCi$logUb95Rr)
        one$calP <- caliP
        return(one)
      }
      subset <- lapply(unique(subset$leaveOutUnit), calibrateLeaveOneOut)
      subset <- do.call("rbind", subset)
    }
    subset$leaveOutUnit <- NULL
    subset$rootOutcomeName <- NULL
    return(subset)
  }
  ParallelLogger::logInfo("Calibrating estimates using leave-one-out")
  cluster <- ParallelLogger::makeCluster(4)
  # calibratedEstimates <- sapply(1:nrow(combis), calibrate, combis = combis, estimates = estimates, simplify = FALSE)
  calibratedEstimates <- ParallelLogger::clusterApply(cluster,
                                                      1:nrow(combis), 
                                                      calibrate, 
                                                      combis = combis, 
                                                      estimates = estimates)
  calibratedEstimates <- do.call("rbind", calibratedEstimates)
  ParallelLogger::stopCluster(cluster)
  normMethod <- gsub("[^a-zA-Z]", "", analysisRef$method[1])
  normDatabase <- gsub("[^a-zA-Z]", "", databaseName)
  estimatesFileName <- file.path(exportFolder, sprintf("estimates_%s_%s.csv", normMethod, normDatabase))
  analysisRefFileName <- file.path(exportFolder, sprintf("analysisRef_%s.csv", normMethod))
  write.csv(calibratedEstimates, estimatesFileName, row.names = FALSE)  
  write.csv(analysisRef, analysisRefFileName, row.names = FALSE)
  ParallelLogger::logInfo("Estimates have been written to ", estimatesFileName)
  ParallelLogger::logInfo("Analysis reference has been written to ", estimatesFileName)
}

# exportFolder <- "r:/MethodsLibraryPleEvaluation_ccae/export"

#' Generate perfomance metrics for the OHDSI Methods Benchmark
#'
#' @param exportFolder    The folder containing the CSV files created using the \code{\link{packageOhdsiBenchmarkResults}}
#'                        function. This folder can contain results from various methods, analyses, and databases.
#' @param mdrr            The minimum detectable relative risk (MDRR). Only controls with this MDRR will be used to compute
#'                        the performance metrics. Set to "All" to include all controls.
#' @param stratum         The stratum for which to compute the metrics, e.g. 'Acute Pancreatitis'. Set to 'All' to use all
#'                        controls.
#' @param trueEffectSize  Should the analysis be limited to a specific true effect size? Set to "Overall" to include all.
#' @param calibrated      Should confidence intervals and p-values be empirically calibrated before computing the metrics?
#' @param comparative     Should the methods be evaluated on the task of comprative effect estimation? If FALSE, they will
#'                        be evaluated on the task of effect estimation.
#'
#' @return
#' A data frame with the various metrics per method - analysisId - database combination.
#' 
#' @export
computeOhdsiBenchmarkMetrics <- function(exportFolder, mdrr = 1.25, stratum = "All", trueEffectSize = "Overall", calibrated = FALSE, comparative = FALSE) {
  
  # Load and prepare estimates of all methods
  files <- list.files(exportFolder, "estimates.*csv", full.names = TRUE)
  estimates <- lapply(files, read.csv)
  estimates <- do.call("rbind", estimates)
  estimates$trueEffectSize[estimates$firstExposureOnly] <- estimates$trueEffectSizeFirstExposure[estimates$firstExposureOnly]
  estimates$trueEffectSize[is.na(estimates$trueEffectSize)] <- estimates$targetEffectSize[is.na(estimates$trueEffectSize)]
  z <- estimates$logRr/estimates$seLogRr
  estimates$p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
  idx <- is.na(estimates$logRr) | is.infinite(estimates$logRr) | is.na(estimates$seLogRr) | is.infinite(estimates$seLogRr)
  estimates$logRr[idx] <- 0
  estimates$seLogRr[idx] <- 999
  estimates$ci95Lb[idx] <- 0
  estimates$ci95Ub[idx] <- 999
  estimates$p[idx] <- 1
  idx <- is.na(estimates$calLogRr) | is.infinite(estimates$calLogRr) | is.na(estimates$calSeLogRr) | is.infinite(estimates$calSeLogRr)
  estimates$calLogRr[idx] <- 0
  estimates$calSeLogRr[idx] <- 999
  estimates$calCi95Lb[idx] <- 0
  estimates$calCi95Ub[idx] <- 999
  estimates$calP[is.na(estimates$calP)] <- 1

  # Load and prepare analysis refs
  files <- list.files(exportFolder, "analysisRef.*csv", full.names = TRUE)
  analysisRef <- lapply(files, read.csv)
  analysisRef <- do.call("rbind", analysisRef)
  
  # Apply selection criteria
  subset <- estimates
  if (mdrr != "All") {
    subset <- subset[!is.na(subset$mdrrTarget) & subset$mdrrTarget < as.numeric(mdrr), ]
    if (comparative) {
      subset <- subset[!is.na(subset$mdrrComparator) & subset$mdrrComparator < as.numeric(input$mdrr), ]
    }
  }
  if (stratum != "All") {
    subset <- subset[subset$stratum == stratum, ]
  }
  if (calibrated) {
    subset$logRr <- subset$calLogRr
    subset$seLogRr <- subset$calSeLogRr
    subset$ci95Lb <- subset$calCi95Lb
    subset$ci95Ub <- subset$calCi95Ub
    subset$p <- subset$calP
  }
  
  # Compute metrics
  combis <- unique(subset[, c("database", "method", "analysisId")])
  if (trueEffectSize == "Overall") {
    computeMetrics <- function(i) {
      forEval <- subset[subset$method == combis$method[i] & subset$analysisId == combis$analysisId[i], ]
      nonEstimable <- round(mean(forEval$seLogRr >= 99), 2)
      roc <- pROC::roc(forEval$targetEffectSize > 1, forEval$logRr, algorithm = 3)
      auc <- round(pROC::auc(roc), 2)
      mse <- round(mean((forEval$logRr - log(forEval$trueEffectSize))^2), 2)
      coverage <- round(mean(forEval$ci95Lb < forEval$trueEffectSize & forEval$ci95Ub > forEval$trueEffectSize), 2)
      meanP <- round(-1 + exp(mean(log(1 + (1/(forEval$seLogRr^2))))), 2)
      type1 <- round(mean(forEval$p[forEval$targetEffectSize == 1] < 0.05), 2)
      type2 <- round(mean(forEval$p[forEval$targetEffectSize > 1] >= 0.05), 2)
      # idx <- forEval$seLogRr < 99
      # meanVarEstimable <- mean(log(forEval$seLogRr[idx] ^ 2))
      # errors <- forEval$logRr[idx] - log(forEval$trueEffectSize[idx])
      # mseEstimable <-  mean(errors^2)
      # varErrorEstimable <- mean((errors - mean(errors)) ^ 2)
      # biasEstimable <- sqrt(mseEstimable - varErrorEstimable)
      return(c(auc = auc, 
               coverage = coverage, 
               meanP = meanP, 
               mse = mse, 
               type1 = type1, 
               type2 = type2, 
               nonEstimable = nonEstimable))
    }
    combis <- cbind(combis, as.data.frame(t(sapply(1:nrow(combis), computeMetrics))))
  } else {
    # trueRr <- input$trueRr
    computeMetrics <- function(i) {
      forEval <- subset[subset$method == combis$method[i] & subset$analysisId == combis$analysisId[i] & subset$targetEffectSize == trueEffectSize, ]
      mse <- round(mean((forEval$logRr - log(forEval$trueEffectSize))^2), 2)
      coverage <- round(mean(forEval$ci95Lb < forEval$trueEffectSize & forEval$ci95Ub > forEval$trueEffectSize), 2)
      meanP <- round(-1 + exp(mean(log(1 + (1/(forEval$seLogRr^2))))), 2)
      if (trueEffectSize == 1) {
        auc <- NA
        type1 <- round(mean(forEval$p < 0.05), 2)  
        type2 <- NA
        nonEstimable <- round(mean(forEval$seLogRr == 999), 2)
      } else {
        negAndPos <- subset[subset$method == combis$method[i] & subset$analysisId == combis$analysisId[i] & (subset$targetEffectSize == trueEffectSize | subset$targetEffectSize == 1), ]
        roc <- pROC::roc(negAndPos$targetEffectSize > 1, negAndPos$logRr, algorithm = 3)
        auc <- round(pROC::auc(roc), 2)
        type1 <- NA
        type2 <- round(mean(forEval$p[forEval$targetEffectSize > 1] >= 0.05), 2)  
        nonEstimable <- round(mean(forEval$seLogRr == 999), 2)
      }
      return(c(auc = auc, coverage = coverage, meanP = meanP, mse = mse, type1 = type1, type2 = type2, nonEstimable = nonEstimable))
    }
    combis <- cbind(combis, as.data.frame(t(sapply(1:nrow(combis), computeMetrics))))
  }
  result <- merge(combis, analysisRef[, c("method", "analysisId", "description")])
  result <- result[order(result$database, result$method, result$analysisId), ]
  result <- result[, c("database", "method", "analysisId", "description", "auc", "coverage", "meanP", "mse", "type1", "type2", "nonEstimable")]
  return(result)
}


# write.csv(result, "r:/MethodsLibraryPleEvaluation_ccae/metrics.csv", row.names = FALSE)
