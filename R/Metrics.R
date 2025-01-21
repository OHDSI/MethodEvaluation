# @file Metrics.R
#
# Copyright 2023 Observational Health Data Sciences and Informatics
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

#' Compute method performance metrics
#'
#' @details
#' Compute the AUC, coverage, mean precision, MSE, type 1 error, type 2 error, and the fraction non-
#' estimable.
#'
#' @param logRr       A numeric vector of effect estimates on the log scale.
#' @param seLogRr     The standard error of the log of the effect estimates. Hint: often the standard
#'                    error = (log(<lower bound 95 percent confidence interval>) - log(<effect
#'                    estimate>))/qnorm(0.025). If not provided the standard error will be inferred from
#'                    the 95 percent confidence interval.
#' @param ci95Lb      The lower bound of the 95 percent confidence interval. IF not provided it will be
#'                    inferred from the standard error.
#' @param ci95Ub      The upper bound of the 95 percent confidence interval. IF not provided it will be
#'                    inferred from the standard error.
#' @param p           The two-sided p-value corresponding to the null hypothesis of no effect. IF not
#'                    provided it will be inferred from the standard error.
#' @param trueLogRr   A vector of the true effect sizes
#'
#' @examples
#' library(EmpiricalCalibration)
#' data <- simulateControls(n = 50 * 3, trueLogRr = log(c(1, 2, 4)))
#' computeMetrics(logRr = data$logRr, seLogRr = data$seLogRr, trueLogRr = data$trueLogRr)
#'
#' @export
computeMetrics <- function(logRr, seLogRr = NULL, ci95Lb = NULL, ci95Ub = NULL, p = NULL, trueLogRr) {
  # data <- EmpiricalCalibration::simulateControls(n = 50 * 3, mean = 0.25, sd = 0.25, trueLogRr =
  # log(c(1, 2, 4))); logRr <- data$logRr; seLogRr <- data$seLogRr; trueLogRr <- data$trueLogRr
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertNumeric(logRr, min.len = 1, add = errorMessages)
  checkmate::assertNumeric(seLogRr, len = length(logRr), null.ok = TRUE, add = errorMessages)
  checkmate::assertNumeric(ci95Lb, len = length(logRr), null.ok = TRUE, add = errorMessages)
  checkmate::assertNumeric(ci95Ub, len = length(logRr), null.ok = TRUE, add = errorMessages)
  checkmate::assertNumeric(p, len = length(logRr), null.ok = TRUE, add = errorMessages)
  checkmate::assertNumeric(trueLogRr, len = length(logRr), add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  if (is.null(seLogRr) && is.null(ci95Lb)) {
    stop("Must specify either standard error or confidence interval")
  }
  data <- data.frame(
    logRr = logRr,
    trueLogRr = trueLogRr
  )
  if (is.null(seLogRr)) {
    data$seLogRr <- (log(ci95Ub) - log(ci95Lb)) / (2 * qnorm(0.975))
  } else {
    data$seLogRr <- seLogRr
  }
  if (is.null(ci95Lb)) {
    data$ci95Lb <- exp(data$logRr + qnorm(0.025) * data$seLogRr)
    data$ci95Ub <- exp(data$logRr + qnorm(0.975) * data$seLogRr)
  } else {
    data$ci95Lb <- ci95Lb
    data$ci95Ub <- ci95Ub
  }
  if (is.null(p)) {
    z <- data$logRr / data$seLogRr
    data$p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
  } else {
    data$p <- p
  }
  
  idx <- is.na(data$logRr) | is.infinite(data$logRr) | is.na(data$seLogRr) | is.infinite(data$seLogRr)
  data$logRr[idx] <- 0
  data$seLogRr[idx] <- 999
  data$ci95Lb[idx] <- 0
  data$ci95Ub[idx] <- 999
  data$p[idx] <- 1
  
  nonEstimable <- round(mean(data$seLogRr >= 99), 2)
  positive <- data$trueLogRr > 0
  if (all(positive) | all(!positive)) {
    auc <- NA
  } else {
    roc <- pROC::roc(positive, data$logRr, algorithm = 3)
    auc <- round(pROC::auc(roc), 2)
  }
  mse <- round(mean((data$logRr - data$trueLogRr)^2), 2)
  coverage <- round(
    mean(data$ci95Lb < exp(data$trueLogRr) & data$ci95Ub > exp(data$trueLogRr)),
    2
  )
  meanP <- round(-1 + exp(mean(log(1 + (1 / (data$seLogRr^2))))), 2)
  type1 <- round(mean(data$p[data$trueLogRr == 0] < 0.05), 2)
  type2 <- round(mean(data$p[data$trueLogRr > 0] >= 0.05), 2)
  return(c(
    auc = auc,
    coverage = coverage,
    meanP = meanP,
    mse = mse,
    type1 = type1,
    type2 = type2,
    nonEstimable = nonEstimable
  ))
}

#' Package results of a method on the OHDSI Methods Benchmark
#'
#' @description
#' Stores the results of a method on the OHDSI Methods Benchmark in a standardized format, for example
#' for use in the Method Evaluation Shiny app.
#'
#' @param estimates        A data frame containing the estimates. See details for required columns.
#' @param controlSummary   A data frame with the summary of the controls as generated by the
#'                         \code{\link{synthesizeReferenceSetPositiveControls}} function.
#' @param analysisRef      A file describing the various analyses that were performed. See details for
#'                         required columns.
#' @param databaseName     A character string to identify the database the method was executed on.
#' @param exportFolder     The folder where the output CSV files will written.
#' @param referenceSet     The name of the reference set for which to package the results. Currently
#'                         supported are "ohdsiMethodsBenchmark" and "ohdsiDevelopment".
#'
#' @details
#' The \code{estimates} argument should have the following columns: "targetId", "outcomeId",
#' "analysisId", "logRr", "seLogRr", "ci95Lb", "ci95Ub".
#' The \code{analysisRef} argument should have the following columns: "analysisId", "method",
#' "comparative", "nesting", "firstExposureOnly"
#' The \code{targetId} and \code{outcomeId} fields identify the specific control, and should
#' correspond to those in the controlSummary object.
#' The \code{analysisId} field is an integer that identifies a specific variant of the method. For
#' example, if the method is 'CohortMethod', analysisId = 1 could identify a set of settings using
#' propensity score matching, and analysisId = 2 could identify a set of settings using
#' stratification.
#' \code{logRr}, \code{seLogRr}, \code{ci95Lb}, and \code{ci95Ub} correspond to the log of the effect
#' estimate (e.g. the hazard ratio), the standard error, and the upper and lower bound of the effect
#' size estimate, as produced by the method.
#' \code{method} is a character string identifyign the method (e.g. "CohortMethod").
#' \code{comparative} is a boolean indicating whether the analysis can also be considerd to perform
#' comparative effect estimation (comparing the target to the comparator).
#' \code{nesting} is a boolean indicating whether the analysis is nested in the nesting cohorts
#' identified in the gold standard.
#' \code{firstExposureOnly} is a boolean indicating whether only the first exposure was used in the
#' analysis.
#'
#' @export
packageOhdsiBenchmarkResults <- function(estimates,
                                         controlSummary,
                                         analysisRef,
                                         databaseName,
                                         exportFolder,
                                         referenceSet = "ohdsiMethodsBenchmark") {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(estimates, add = errorMessages)
  checkmate::assertNames(
    colnames(estimates),
    must.include = c(
      "targetId",
      "outcomeId",
      "analysisId",
      "logRr",
      "seLogRr",
      "ci95Lb",
      "ci95Ub"
    ),
    add = errorMessages
  )
  checkmate::assertDataFrame(controlSummary, add = errorMessages)
  checkmate::assertNames(
    colnames(controlSummary),
    must.include = c(
      "outcomeId",
      "comparatorId",
      "targetId",
      "targetName",
      "comparatorName",
      "nestingId",
      "nestingName",
      "outcomeName",
      "type",
      "targetEffectSize",
      "trueEffectSize",
      "trueEffectSizeFirstExposure",
      "oldOutcomeId",
      "mdrrTarget",
      "mdrrComparator"
    ),
    add = errorMessages
  )
  checkmate::assertDataFrame(analysisRef, add = errorMessages)
  checkmate::assertNames(
    colnames(analysisRef),
    must.include = c(
      "analysisId",
      "method",
      "comparative",
      "nesting",
      "firstExposureOnly"
    ),
    add = errorMessages
  )
  checkmate::assertChoice(referenceSet, choices = c("ohdsiMethodsBenchmark", "ohdsiDevelopment"), add = errorMessages)
  checkmate::assertCharacter(databaseName, len = 1, add = errorMessages)
  checkmate::assertCharacter(exportFolder, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  if (!file.exists(exportFolder)) {
    dir.create(exportFolder, recursive = TRUE)
  }
  
  # Create full grid of controls (including those that did not make it in the database:
  if (referenceSet == "ohdsiMethodsBenchmark") {
    ohdsiNegativeControls <- readRDS(system.file("ohdsiNegativeControls.rds",
                                                 package = "MethodEvaluation"
    ))
  } else {
    ohdsiNegativeControls <- readRDS(system.file("ohdsiDevelopmentNegativeControls.rds",
                                                 package = "MethodEvaluation"
    ))
  }
  ohdsiNegativeControls$oldOutcomeId <- ohdsiNegativeControls$outcomeId
  ohdsiNegativeControls$stratum <- ohdsiNegativeControls$outcomeName
  idx <- ohdsiNegativeControls$type == "Outcome control"
  ohdsiNegativeControls$stratum[idx] <- ohdsiNegativeControls$targetName[idx]
  ohdsiNegativeControls <- ohdsiNegativeControls[, c(
    "targetId",
    "targetName",
    "comparatorId",
    "comparatorName",
    "nestingId",
    "nestingName",
    "oldOutcomeId",
    "outcomeName",
    "type",
    "stratum"
  )]
  fullGrid <- do.call("rbind", replicate(4, ohdsiNegativeControls, simplify = FALSE))
  fullGrid$targetEffectSize <- rep(c(1, 1.5, 2, 4), each = nrow(ohdsiNegativeControls))
  idx <- fullGrid$targetEffectSize != 1
  fullGrid$outcomeName[idx] <- paste0(
    fullGrid$outcomeName[idx],
    ", RR=",
    fullGrid$targetEffectSize[idx]
  )
  allControls <- merge(controlSummary, fullGrid, all.y = TRUE)
  
  .packageBenchmarkResults(
    allControls = allControls,
    analysisRef = analysisRef,
    estimates = estimates,
    exportFolder = exportFolder,
    databaseName = databaseName
  )
}

.packageBenchmarkResults <- function(allControls,
                                     analysisRef,
                                     estimates,
                                     exportFolder,
                                     databaseName) {
  # Merge estimates into full grid:
  analysisIds <- unique(analysisRef$analysisId)
  fullGrid <- lapply(analysisIds, function(x) mutate(allControls, analysisId = x)) %>%
    bind_rows()
  estimates <- estimates %>%
    select(
      "targetId",
      "outcomeId",
      "analysisId",
      "logRr",
      "seLogRr",
      "ci95Lb",
      "ci95Ub"
    ) %>%
    right_join(fullGrid, by = join_by("targetId", "outcomeId", "analysisId")) %>%
    inner_join(
      analysisRef %>%
        select("analysisId", "method", "comparative", "nesting", "firstExposureOnly"),
      by = join_by("analysisId")
    ) %>%
    mutate(database = databaseName)
  
  # Perform empirical calibration:
  # subset = subsets[[5]]
  calibrate <- function(subset) {
    subset <- subset %>%
      mutate(leaveOutUnit = if_else(.data$type == "Exposure control",
                                    paste(.data$targetId, .data$oldOutcomeId),
                                    as.character(.data$oldOutcomeId)))
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
        model <- EmpiricalCalibration::fitSystematicErrorModel(
          logRr = subsetMinusOne$logRr,
          seLogRr = subsetMinusOne$seLogRr,
          trueLogRr = log(subsetMinusOne$targetEffectSize),
          estimateCovarianceMatrix = FALSE
        )
        caliCi <- EmpiricalCalibration::calibrateConfidenceInterval(
          logRr = one$logRr,
          seLogRr = one$seLogRr,
          model = model
        )
        null <- EmpiricalCalibration::fitNull(logRr = subsetMinusOne$logRr[subsetMinusOne$targetEffectSize ==
                                                                             1], seLogRr = subsetMinusOne$seLogRr[subsetMinusOne$targetEffectSize == 1])
        caliP <- EmpiricalCalibration::calibrateP(
          null = null,
          logRr = one$logRr,
          seLogRr = one$seLogRr
        )
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
    return(subset)
  }
  ParallelLogger::logInfo("Calibrating estimates using leave-one-out")
  cluster <- ParallelLogger::makeCluster(4)
  ParallelLogger::clusterRequire(cluster, "dplyr")
  subsets <- split(estimates, paste(estimates$method, estimates$analysisId, estimates$stratum))
  calibratedEstimates <- ParallelLogger::clusterApply(
    cluster,
    subsets,
    calibrate
  )
  calibratedEstimates <- bind_rows(calibratedEstimates)
  ParallelLogger::stopCluster(cluster)
  normMethod <- gsub("[^a-zA-Z]", "", analysisRef$method[1])
  normDatabase <- gsub("[^a-zA-Z]", "", databaseName)
  estimatesFileName <- file.path(
    exportFolder,
    sprintf("estimates_%s_%s.csv", normMethod, normDatabase)
  )
  analysisRefFileName <- file.path(exportFolder, sprintf("analysisRef_%s.csv", normMethod))
  readr::write_csv(calibratedEstimates, estimatesFileName)
  readr::write_csv(analysisRef, analysisRefFileName)
  ParallelLogger::logInfo("Estimates have been written to ", estimatesFileName)
  ParallelLogger::logInfo("Analysis reference has been written to ", analysisRefFileName)
}

#' Package results of a method on the OHDSI Methods Benchmark
#'
#' @description
#' Stores the results of a method on the OHDSI Methods Benchmark in a standardized format, for example
#' for use in the Method Evaluation Shiny app.
#'
#' @param estimates        A data frame containing the estimates. See details for required columns.
#' @param negativeControls A data frame containing the negative controls. See details for required columns.
#' @param synthesisSummary A data frame with the summary of the positive control synthesis as generated by the
#'                         \code{\link{synthesizePositiveControls}} function.
#' @param mdrr             The MDRR as computed using the \code{\link{computeMdrr}} function. Should contain the
#'                         following columns: "exposureId", "outcomeId", "mdrr". For comparative analyses,
#'                         the "mdrr" column can be replaced by a "mdrrTarget" and "mdrrComparator" column.
#' @param analysisRef      A file describing the various analyses that were performed. See details for
#'                         required columns.
#' @param databaseName     A character string to identify the database the method was executed on.
#' @param exportFolder     The folder where the output CSV files will written.
#'
#' @details
#' The \code{estimates} argument should have the following columns: "targetId", "outcomeId",
#' "analysisId", "logRr", "seLogRr", "ci95Lb", "ci95Ub".
#' The \code{negativeControls} argument should have the following columns: "targetId", "outcomeId", "type".
#' The \code{analysisRef} argument should have the following columns: "analysisId", "method",
#' "comparative", "nesting", "firstExposureOnly"
#' The \code{targetId} and \code{outcomeId} fields identify the specific control, and should
#' correspond to those in the negativeControls and synthesisSummary objects.
#' The \code{type} fields should be either "Outcome control" or "Exposure control".
#' The \code{analysisId} field is an integer that identifies a specific variant of the method. For
#' example, if the method is 'CohortMethod', analysisId = 1 could identify a set of settings using
#' propensity score matching, and analysisId = 2 could identify a set of settings using
#' stratification.
#' \code{logRr}, \code{seLogRr}, \code{ci95Lb}, and \code{ci95Ub} correspond to the log of the effect
#' estimate (e.g. the hazard ratio), the standard error, and the upper and lower bound of the effect
#' size estimate, as produced by the method.
#' \code{method} is a character string identifyign the method (e.g. "CohortMethod").
#' \code{comparative} is a boolean indicating whether the analysis can also be considerd to perform
#' comparative effect estimation (comparing the target to the comparator).
#' \code{nesting} is a boolean indicating whether the analysis is nested in the nesting cohorts
#' identified in the gold standard.
#' \code{firstExposureOnly} is a boolean indicating whether only the first exposure was used in the
#' analysis.
#'
#' @export
packageCustomBenchmarkResults <- function(estimates,
                                          negativeControls,
                                          synthesisSummary,
                                          mdrr = NULL,
                                          analysisRef,
                                          databaseName,
                                          exportFolder) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertDataFrame(estimates, add = errorMessages)
  checkmate::assertNames(
    colnames(estimates),
    must.include = c(
      "targetId",
      "outcomeId",
      "analysisId",
      "logRr",
      "seLogRr",
      "ci95Lb",
      "ci95Ub"
    ),
    add = errorMessages
  )
  checkmate::assertDataFrame(negativeControls, add = errorMessages)
  checkmate::assertNames(
    colnames(negativeControls),
    must.include = c(
      "targetId",
      "outcomeId",
      "type"
    ),
    add = errorMessages
  )
  checkmate::assertDataFrame(synthesisSummary, add = errorMessages)
  checkmate::assertNames(
    colnames(synthesisSummary),
    must.include = c(
      "exposureId", 
      "outcomeId", 
      "targetEffectSize", 
      "newOutcomeId", 
      "trueEffectSize", 
      "trueEffectSizeFirstExposure", 
      "trueEffectSizeItt"
    ),
    add = errorMessages
  )
  checkmate::assertDataFrame(mdrr, null.ok = TRUE, add = errorMessages)
  if (!is.null(mdrr)) {
    checkmate::assertNames(
      colnames(mdrr),
      must.include = c(
        "exposureId", 
        "outcomeId"
      ),
      add = errorMessages
    )
  }
  checkmate::assertDataFrame(analysisRef, add = errorMessages)
  checkmate::assertNames(
    colnames(analysisRef),
    must.include = c(
      "analysisId",
      "method",
      "comparative",
      "nesting",
      "firstExposureOnly"
    ),
    add = errorMessages
  )
  checkmate::assertCharacter(databaseName, len = 1, add = errorMessages)
  checkmate::assertCharacter(exportFolder, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  trueEffecSizes <- c(1, unique(synthesisSummary$targetEffectSize))
  negativeControls <- negativeControls %>%
    mutate(stratum = if_else(.data$type == "Outcome control", .data$targetId, .data$outcomeId)) %>%
    rename(oldOutcomeId = "outcomeId")
  fullGrid <- lapply(trueEffecSizes, function(x) mutate(negativeControls, targetEffectSize = x)) %>%
    bind_rows()
  synthesisSummary <- synthesisSummary %>%
    rename(targetId = "exposureId") %>%
    rename(oldOutcomeId = "outcomeId") %>%
    rename(outcomeId = "newOutcomeId") %>%
    select("targetId", "oldOutcomeId", "targetEffectSize", "outcomeId", "trueEffectSize", "trueEffectSizeFirstExposure", "trueEffectSizeItt")
  allControls <- left_join(
    fullGrid,
    synthesisSummary,
    by = join_by("targetId", "oldOutcomeId", "targetEffectSize")
  ) %>%
    mutate(outcomeId = if_else(.data$targetEffectSize == 1, .data$oldOutcomeId, .data$outcomeId))
  if (is.null(mdrr)) {
    allControls <- allControls %>%
      mutate(mdrrTarget = as.numeric(NA), mdrrComparator = as.numeric(NA))
  } else {
    if (!"mdrrTarget" %in% colnames(mdrr)) {
      mdrr <- mdrr %>%
        mutate(mdrrTarget = .data$mdrr, mdrrComparator = .data$mdrr)
    }
    allControls <- allControls %>%
      left_join(
        mdrr %>% 
          rename(targetId = "exposureId"), 
        by = c("targetId", "outcomeId")
      )
  }
  .packageBenchmarkResults(
    allControls = allControls,
    analysisRef = analysisRef,
    estimates = estimates,
    exportFolder = exportFolder,
    databaseName = databaseName
  )
}

# exportFolder <- 'r:/MethodsLibraryPleEvaluation_ccae/export'

#' Generate perfomance metrics for the OHDSI Methods Benchmark
#'
#' @param exportFolder     The folder containing the CSV files created using the
#'                         \code{\link{packageOhdsiBenchmarkResults}} function. This folder can contain
#'                         results from various methods, analyses, and databases.
#' @param mdrr             The minimum detectable relative risk (MDRR). Only controls with this MDRR
#'                         will be used to compute the performance metrics. Set to "All" to include all
#'                         controls.
#' @param stratum          The stratum for which to compute the metrics, e.g. 'Acute Pancreatitis'. Set
#'                         to 'All' to use all controls.
#' @param trueEffectSize   Should the analysis be limited to a specific true effect size? Set to
#'                         "Overall" to include all.
#' @param calibrated       Should confidence intervals and p-values be empirically calibrated before
#'                         computing the metrics?
#' @param comparative      Should the methods be evaluated on the task of comprative effect estimation?
#'                         If FALSE, they will be evaluated on the task of effect estimation.
#'
#' @return
#' A data frame with the various metrics per method - analysisId - database combination.
#'
#' @export
computeOhdsiBenchmarkMetrics <- function(exportFolder,
                                         mdrr = 1.25,
                                         stratum = "All",
                                         trueEffectSize = "Overall",
                                         calibrated = FALSE,
                                         comparative = FALSE) {
  errorMessages <- checkmate::makeAssertCollection()
  checkmate::assertCharacter(exportFolder, len = 1, add = errorMessages)
  checkmate::assertNumeric(mdrr, len = 1, add = errorMessages)
  checkmate::assertAtomic(stratum, len = 1, add = errorMessages)
  checkmate::assertAtomic(trueEffectSize, len = 1, add = errorMessages)
  checkmate::assertLogical(calibrated, len = 1, add = errorMessages)
  checkmate::assertLogical(comparative, len = 1, add = errorMessages)
  checkmate::reportAssertions(collection = errorMessages)
  
  # Load and prepare estimates of all methods
  files <- list.files(exportFolder, "estimates.*csv", full.names = TRUE)
  estimates <- lapply(files, read.csv)
  estimates <- do.call("rbind", estimates)
  estimates$trueEffectSize[estimates$firstExposureOnly] <- estimates$trueEffectSizeFirstExposure[estimates$firstExposureOnly]
  estimates$trueEffectSize[is.na(estimates$trueEffectSize)] <- estimates$targetEffectSize[is.na(estimates$trueEffectSize)]
  z <- estimates$logRr / estimates$seLogRr
  estimates$p <- 2 * pmin(pnorm(z), 1 - pnorm(z))
  idx <- is.na(estimates$logRr) | is.infinite(estimates$logRr) | is.na(estimates$seLogRr) | is.infinite(estimates$seLogRr)
  estimates$logRr[idx] <- 0
  estimates$seLogRr[idx] <- 999
  estimates$ci95Lb[idx] <- 0
  estimates$ci95Ub[idx] <- 999
  estimates$p[idx] <- 1
  idx <- is.na(estimates$calLogRr) | is.infinite(estimates$calLogRr) | is.na(estimates$calSeLogRr) |
    is.infinite(estimates$calSeLogRr)
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
      subset <- subset[!is.na(subset$mdrrComparator) & subset$mdrrComparator < as.numeric(mdrr), ]
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
      coverage <- round(
        mean(forEval$ci95Lb < forEval$trueEffectSize & forEval$ci95Ub > forEval$trueEffectSize),
        2
      )
      meanP <- round(-1 + exp(mean(log(1 + (1 / (forEval$seLogRr^2))))), 2)
      type1 <- round(mean(forEval$p[forEval$targetEffectSize == 1] < 0.05), 2)
      type2 <- round(mean(forEval$p[forEval$targetEffectSize > 1] >= 0.05), 2)
      return(c(
        auc = auc,
        coverage = coverage,
        meanP = meanP,
        mse = mse,
        type1 = type1,
        type2 = type2,
        nonEstimable = nonEstimable
      ))
    }
    combis <- cbind(combis, as.data.frame(t(sapply(1:nrow(combis), computeMetrics))))
  } else {
    # trueRr <- input$trueRr
    computeMetrics <- function(i) {
      forEval <- subset[subset$method == combis$method[i] & subset$analysisId == combis$analysisId[i] &
                          subset$targetEffectSize == trueEffectSize, ]
      mse <- round(mean((forEval$logRr - log(forEval$trueEffectSize))^2), 2)
      coverage <- round(
        mean(forEval$ci95Lb < forEval$trueEffectSize & forEval$ci95Ub > forEval$trueEffectSize),
        2
      )
      meanP <- round(-1 + exp(mean(log(1 + (1 / (forEval$seLogRr^2))))), 2)
      if (trueEffectSize == 1) {
        auc <- NA
        type1 <- round(mean(forEval$p < 0.05), 2)
        type2 <- NA
        nonEstimable <- round(mean(forEval$seLogRr == 999), 2)
      } else {
        negAndPos <- subset[subset$method == combis$method[i] & subset$analysisId == combis$analysisId[i] &
                              (subset$targetEffectSize == trueEffectSize | subset$targetEffectSize == 1), ]
        roc <- pROC::roc(negAndPos$targetEffectSize > 1, negAndPos$logRr, algorithm = 3)
        auc <- round(pROC::auc(roc), 2)
        type1 <- NA
        type2 <- round(mean(forEval$p[forEval$targetEffectSize > 1] >= 0.05), 2)
        nonEstimable <- round(mean(forEval$seLogRr == 999), 2)
      }
      return(c(
        auc = auc,
        coverage = coverage,
        meanP = meanP,
        mse = mse,
        type1 = type1,
        type2 = type2,
        nonEstimable = nonEstimable
      ))
    }
    combis <- cbind(combis, as.data.frame(t(sapply(1:nrow(combis), computeMetrics))))
  }
  result <- merge(combis, analysisRef[, c("method", "analysisId", "description")])
  result <- result[order(result$database, result$method, result$analysisId), ]
  result <- result[, c(
    "database",
    "method",
    "analysisId",
    "description",
    "auc",
    "coverage",
    "meanP",
    "mse",
    "type1",
    "type2",
    "nonEstimable"
  )]
  return(result)
}


# write.csv(result, 'r:/MethodsLibraryPleEvaluation_ccae/metrics.csv', row.names = FALSE)
