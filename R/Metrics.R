# @file Metrics.R
#
# Copyright 2017 Observational Health Data Sciences and Informatics
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

#' Compute the area under the ROC curve
#'
#' @export
computeAuc <- function(methodResults, referenceSet, confidenceIntervals = TRUE) {
  # TODO: add stratification (e.g. by analysisId, outcomeConceptId)
  methodResults <- merge(methodResults,
                         referenceSet[,
                                      c("exposureConceptId", "outcomeConceptId", "groundTruth")])
  roc <- pROC::roc.default(methodResults$groundTruth, methodResults$logRr, algorithm = 3)
  if (confidenceIntervals) {
    auc <- as.numeric(pROC::ci.auc.roc(roc, method = "delong"))
    return(data.frame(auc = auc[2], auc_lb95ci = auc[1], auc_lb95ci = auc[3]))
  } else {
    auc <- pROC::auc.roc(roc)
    return(auc[1])
  }
}

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
