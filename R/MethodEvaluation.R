# @file MethodEvaluation.R
#
# Copyright 2016 Observational Health Data Sciences and Informatics
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

#' The OMOP reference set
#' A reference set of 165 drug-outcome pairs where we believe the drug causes the outcome ( positive
#' controls) and 234 drug-outcome pairs where we believe the drug does not cause the outcome (negative
#' controls). The controls involve 4 health outcomes of interest: acute liver injury, acute kidney
#' injury, acute myocardial infarction, and GI bleeding.
#'
#' @docType data
#' @keywords datasets
#' @name omopReferenceSet
#' @usage
#' data(omopReferenceSet)
#' @format
#' A data frame with 399 rows and 10 variables: \describe{ \item{exposureConceptId}{Concept ID
#' identifying the exposure} \item{exposureConceptName}{Name of the exposure}
#' \item{outcomeConceptId}{Concept ID identifying the outcome} \item{outcomeConceptName}{Name of the
#' outcome} \item{groundTruth}{0 = negative control, 1 = positive control}
#' \item{indicationConceptId}{Concept Id identifying the (primary) indication of the drug. To be used
#' when one wants to nest the analysis within the indication} \item{indicationConceptName}{Name of the
#' indication} \item{comparatorDrugConceptId}{Concept ID identifying a comparator drug that can be
#' used as a counterfactual} \item{comparatorDrugConceptName}{Name of the comparator drug}
#' \item{comparatorType}{How the comparator was selected} }
#' @references
#' Ryan PB, Schuemie MJ, Welebob E, Duke J, Valentine S, Hartzema AG. Defining a reference set to
#' support methodological research in drug safety. Drug Safety 36 Suppl 1:S33-47, 2013
NULL

#' The EU-ADR reference set
#'
#' A reference set of 43 drug-outcome pairs where we believe the drug causes the outcome (
#' positive controls) and 50 drug-outcome pairs where we believe the drug does not cause the 
#' outcome (negative controls). The controls involve 10 health outcomes of interest. Note that
#' originally, there was an additional positive control (Nimesulide and acute liver injury), but
#' Nimesulide is not in RxNorm, and is not available in many countries.
#'
#' @docType data
#' @keywords datasets
#' @name euadrReferenceSet
#' @usage
#' data(euadrReferenceSet)
#' @format
#' A data frame with 399 rows and 10 variables: \describe{ \item{exposureConceptId}{Concept ID
#' identifying the exposure} \item{exposureConceptName}{Name of the exposure}
#' \item{outcomeConceptId}{Concept ID identifying the outcome} \item{outcomeConceptName}{Name of the
#' outcome} \item{groundTruth}{0 = negative control, 1 = positive control}
#' \item{indicationConceptId}{Concept Id identifying the (primary) indication of the drug. To be used
#' when one wants to nest the analysis within the indication} \item{indicationConceptName}{Name of the
#' indication} \item{comparatorDrugConceptId}{Concept ID identifying a comparator drug that can be
#' used as a counterfactual} \item{comparatorDrugConceptName}{Name of the comparator drug}
#' \item{comparatorType}{How the comparator was selected} }
#' @references
#' Coloma PM, Avillach P, Salvo F, Schuemie MJ, Ferrajolo C, Pariente A, Fourrier-Réglat A, Molokhia
#' M, Patadia V, van der Lei J, Sturkenboom M, Trifirò G. A reference standard for evaluation of
#' methods for drug safety signal detection using electronic healthcare record databases. Drug Safety
#' 36(1):13-23, 2013
NULL

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

#' Plot the ROC curves for various injected signal sizes
#'
#' @param logRr       A vector containing the log of the relative risk as estimated by a method.
#' @param trueLogRr   A vector containing the injected log(relative risk) for each estimate.
#' @param showAucs    Should the AUCs be shown in the plot?
#' @param fileName    Name of the file where the plot should be saved, for example 'plot.png'. See the
#'                    function \code{ggsave} in the ggplot2 package for supported file formats.
#' 
#' @return
#' A Ggplot object. Use the \code{ggsave} function to save to file.
#' 
#' @export
plotRocsInjectedSignals <- function(logRr, trueLogRr, showAucs, fileName = NULL) {
  trueLogRrLevels <- unique(trueLogRr)
  if (all(trueLogRrLevels != 0))
    stop("Requiring at least one true relative risk of 1")
  
  allData <- data.frame()
  aucs <- c()
  trueRrs <- c()
  for (trueLogRrLevel in trueLogRrLevels){
    if (trueLogRrLevel != 0 ) {
      # trueLogRrLevel <- log(2)
      data <- data.frame(logRr = logRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel], 
                         trueLogRr = trueLogRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel])
      data$truth <- data$trueLogRr != 0
      
      roc <- pROC::roc(data$truth, data$logRr, algorithm = 3)
      if (showAucs) {
        aucs <- c(aucs, pROC::auc(roc))
        trueRrs <- c(trueRrs, exp(trueLogRrLevel))
      }
      data <- data.frame(sens = roc$sensitivities, fpRate = 1 - roc$specificities, trueRr = exp(trueLogRrLevel))
      data <- data[order(data$sens, data$fpRate), ]
      allData <- rbind(allData, data)
    }
  }
  allData$trueRr <- as.factor(allData$trueRr)
  plot <- ggplot2::ggplot(allData, ggplot2::aes(x = fpRate, y = sens, group = trueRr, color = trueRr, fill = trueRr)) +
    ggplot2::geom_abline(intercept = 0, slope = 1) +
    ggplot2::geom_line(alpha = 0.5, size = 1) +
    ggplot2::scale_x_continuous("1 - specificity") +
    ggplot2::scale_y_continuous("Sensitivity")
  
  if (showAucs) {
    aucs <- data.frame(auc = aucs, trueRr = trueRrs) 
    aucs <- aucs[order(-aucs$trueRr), ]
    for (i in 1:nrow(aucs)) {
      label <- paste0("True RR = ",format(round(aucs$trueRr[i], 2), nsmall = 2), ": AUC = ", format(round(aucs$auc[i], 2), nsmall = 2))
      plot <- plot + ggplot2::geom_text(label = label, x = 1, y = (i-1)*0.1, hjust = 1, color = "#000000")
    }
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5.5, height = 4.5, dpi = 400)
  return(plot)
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
computeAucsInjectedSignals <- function(logRr, trueLogRr) {
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

#' Compute the mean squared error
#'
#' @export
computeMse <- function() {

}

#' Compute the bias (mean error)
#'
#' @export
computeBias <- function() {

}

#' Plot the error distribution
#'
#' @export
plotBias <- function() {

}


