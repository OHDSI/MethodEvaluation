# @file Plots.R
#
# Copyright 2021 Observational Health Data Sciences and Informatics
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
  if (any(is.na(logRr))) {
    warning("Some estimates are NA, removing prior to computing AUCs")
    trueLogRr <- trueLogRr[!is.na(logRr)]
    logRr <- logRr[!is.na(logRr)]
  }
  trueLogRrLevels <- unique(trueLogRr)
  if (all(trueLogRrLevels != 0))
    stop("Requiring at least one true relative risk of 1")
  
  allData <- data.frame()
  aucs <- c()
  trueRrs <- c()
  for (trueLogRrLevel in trueLogRrLevels) {
    if (trueLogRrLevel != 0) {
      data <- data.frame(logRr = logRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel],
                         trueLogRr = trueLogRr[trueLogRr == 0 | trueLogRr == trueLogRrLevel])
      data$truth <- data$trueLogRr != 0
      
      roc <- pROC::roc(data$truth, data$logRr, algorithm = 3)
      if (showAucs) {
        aucs <- c(aucs, pROC::auc(roc))
        trueRrs <- c(trueRrs, exp(trueLogRrLevel))
      }
      data <- data.frame(sens = roc$sensitivities,
                         fpRate = 1 - roc$specificities,
                         trueRr = exp(trueLogRrLevel))
      data <- data[order(data$sens, data$fpRate), ]
      allData <- rbind(allData, data)
    }
  }
  allData$trueRr <- as.factor(allData$trueRr)
  plot <- ggplot2::ggplot(allData, ggplot2::aes(x = .data$fpRate,
                                                y = .data$sens,
                                                group = .data$trueRr,
                                                color = .data$trueRr,
                                                fill = .data$trueRr)) +
    ggplot2::geom_abline(intercept = 0, slope = 1) +
    ggplot2::geom_line(alpha = 0.5, size = 1) +
    ggplot2::scale_x_continuous("1 - specificity") +
    ggplot2::scale_y_continuous("Sensitivity")
  
  if (showAucs) {
    aucs <- data.frame(auc = aucs, trueRr = trueRrs)
    aucs <- aucs[order(-aucs$trueRr), ]
    for (i in 1:nrow(aucs)) {
      label <- paste0("True RR = ",
                      format(round(aucs$trueRr[i], 2), nsmall = 2),
                      ": AUC = ",
                      format(round(aucs$auc[i], 2), nsmall = 2))
      plot <- plot + ggplot2::geom_text(label = label,
                                        x = 1,
                                        y = (i - 1) * 0.1,
                                        hjust = 1,
                                        color = "#000000")
    }
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5.5, height = 4.5, dpi = 400)
  return(plot)
}

#' Plot the coverage
#'
#' @details
#' Plot the fractions of estimates where the true effect size is below, above or within the confidence
#' interval, for one or more true effect sizes.
#'
#' @param logRr       A numeric vector of effect estimates on the log scale
#' @param seLogRr     The standard error of the log of the effect estimates. Hint: often the standard
#'                    error = (log(<lower bound 95 percent confidence interval>) - log(<effect
#'                    estimate>))/qnorm(0.025)
#' @param trueLogRr   A vector of the true effect sizes
#' @param region      Size of the confidence interval. Default is .95 (95 percent).
#' @param fileName    Name of the file where the plot should be saved, for example 'plot.png'. See the
#'                    function \code{ggsave} in the ggplot2 package for supported file formats.
#'
#' @export
plotCoverageInjectedSignals <- function(logRr, seLogRr, trueLogRr, region = 0.95, fileName = NULL) {
  data <- data.frame(logRr = logRr,
                     logLb95Rr = logRr + qnorm((1 - region)/2) * seLogRr,
                     logUb95Rr = logRr + qnorm(1 - (1 - region)/2) * seLogRr,
                     trueLogRr = trueLogRr,
                     trueRr = round(exp(trueLogRr), 2))
  if (any(is.na(data$logRr))) {
    warning("Some estimates are NA, removing prior to computing coverage")
    data <- data[!is.na(data$logRr), ]
  }
  vizD <- data.frame()
  for (trueRr in unique(data$trueRr)) {
    subset <- data[data$trueRr == trueRr, ]
    d <- data.frame(trueRr = trueRr, group = c("Below CI",
                                               "Within CI",
                                               "Above CI"), fraction = 0, pos = 0)
    d$fraction[1] <- mean(subset$trueLogRr < subset$logLb95Rr)
    d$fraction[2] <- mean(subset$trueLogRr >= subset$logLb95Rr & subset$trueLogRr <= subset$logUb95Rr)
    d$fraction[3] <- mean(subset$trueLogRr > subset$logUb95Rr)
    d$pos[1] <- d$fraction[1]/2
    d$pos[2] <- d$fraction[1] + (d$fraction[2]/2)
    d$pos[3] <- d$fraction[1] + d$fraction[2] + (d$fraction[3]/2)
    vizD <- rbind(vizD, d)
  }
  vizD$pos <- sapply(vizD$pos, function(x) {
    min(max(x, 0.05), 0.95)
  })
  
  vizD$label <- paste(round(100 * vizD$fraction), "%", sep = "")
  vizD$group <- factor(vizD$group, levels = c("Below CI", "Within CI", "Above CI"))
  theme <- ggplot2::element_text(colour = "#000000", size = 10)
  plot <- with(vizD, {
    ggplot2::ggplot(vizD, ggplot2::aes(x = as.factor(trueRr), y = fraction)) + 
      ggplot2::geom_bar(ggplot2::aes(fill = group), stat = "identity", position = "stack", alpha = 0.8) + 
      ggplot2::scale_fill_manual(values = c("#174a9f", "#f9dd75", "#f15222")) + 
      ggplot2::geom_text(ggplot2::aes(label = label, y = pos), size = 3) + 
      ggplot2::scale_x_discrete("True relative risk") + 
      ggplot2::scale_y_continuous("Coverage") + 
      ggplot2::theme(panel.grid.minor = ggplot2::element_blank(), 
                     panel.background = ggplot2::element_rect(fill = "#FAFAFA", colour = NA), 
                     panel.grid.major = ggplot2::element_blank(), 
                     axis.ticks = ggplot2::element_blank(), 
                     axis.text.y = ggplot2::element_blank(), 
                     axis.text.x = theme, 
                     legend.key = ggplot2::element_blank(), 
                     legend.position = "right")
  })
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 5, height = 3.5, dpi = 400)
  return(plot)
}

#' Plot negative and positive control estimates.
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
#' @param trueLogRr   A vector of the true effect sizes
#' @param estimateType A character string to denote the effect size estimate type. Used for the x-axis
#'                     and the true effect size labels.
#' @param fileName    Name of the file where the plot should be saved, for example 'plot.png'. See the
#'                    function \code{ggsave} in the ggplot2 package for supported file formats.
#' @param title       An optional title to display above the plot.
#' 
#' @return
#' A Ggplot object. Use the \code{ggsave} function to save to file.
#' 
#' @export
plotControls <- function(logRr, seLogRr = NULL, ci95Lb = NULL, ci95Ub = NULL, trueLogRr, estimateType = "relative risk",  fileName = NULL, title) {
  if (is.null(seLogRr) && is.null(ci95Lb)) {
    stop("Must specify either standard error or confidence interval")
  }
  data <- data.frame(logRr = logRr,
                     trueLogRr = trueLogRr)
  if (is.null(seLogRr)) {
    data$seLogRr <- (log(ci95Ub) - log(ci95Lb)) / (2*qnorm(0.975))
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
  data <- data[!is.na(data$seLogRr), ]
  data$Significant <- data$ci95Lb > exp(data$trueLogRr) | data$ci95Ub < exp(data$trueLogRr)
  data$Group <- as.factor(paste("True", estimateType, "=", exp(data$trueLogRr)))
  temp1 <- aggregate(Significant ~ Group, data = data, length)
  temp2 <- aggregate(Significant ~ Group, data = data, mean)
  
  temp1$nLabel <- paste0(formatC(temp1$Significant, big.mark = ","), " estimates")
  temp1$Significant <- NULL
  
  temp2$meanLabel <- paste0(formatC(100 * (1 - temp2$Significant), digits = 1, format = "f"),
                            "% of CIs includes ",
                            substr(as.character(temp2$Group),
                                   start = 21,
                                   stop = nchar(as.character(temp2$Group))))
  temp2$Significant <- NULL
  dd <- merge(temp1, temp2)
  dd$tes <- as.numeric(substr(as.character(dd$Group),
                              start = 21,
                              stop = nchar(as.character(dd$Group))))
  
  breaks <- c(0.25, 0.5, 1, 2, 4, 6, 8)
  theme <- ggplot2::element_text(colour = "#000000", size = 14)
  themeRA <- ggplot2::element_text(colour = "#000000", size = 14, hjust = 1)
  
  alpha <- 1 - min(0.95 * (nrow(data)/nrow(dd)/50000)^0.1, 0.95)
  plot <- ggplot2::ggplot(data, ggplot2::aes(x = logRr, y = seLogRr)) +
    ggplot2::geom_vline(xintercept = log(breaks), colour = "#CCCCCC", lty = 1, size = 0.5) +
    ggplot2::geom_abline(ggplot2::aes(intercept = (-log(.data$tes))/qnorm(0.025), slope = 1/qnorm(0.025)),
                colour = rgb(0.8, 0, 0),
                linetype = "dashed",
                size = 1,
                alpha = 0.5,
                data = dd) +
    ggplot2::geom_abline(ggplot2::aes(intercept = (-log(.data$tes))/qnorm(0.975), slope = 1/qnorm(0.975)),
                colour = rgb(0.8, 0, 0),
                linetype = "dashed",
                size = 1,
                alpha = 0.5,
                data = dd) +
    ggplot2::geom_point(size = 2, color = rgb(0, 0, 0, alpha = 0.05), alpha = alpha, shape = 16) +
    ggplot2::geom_hline(yintercept = 0) +
    ggplot2::geom_label(x = log(0.26),
               y = 0.96,
               alpha = 1,
               hjust = "left",
               ggplot2::aes(label = .data$nLabel),
               size = 5,
               data = dd) +
    ggplot2::geom_label(x = log(0.26),
               y = 0.8,
               alpha = 1,
               hjust = "left",
               ggplot2::aes(label = .data$meanLabel),
               size = 5,
               data = dd) +
    ggplot2::scale_x_continuous(paste("Estimated", estimateType),
                       limits = log(c(0.25, 10)),
                       breaks = log(breaks),
                       labels = breaks) +
    ggplot2::scale_y_continuous("Standard Error", limits = c(0, 1)) +
    ggplot2::facet_grid(. ~ Group) +
    ggplot2::theme(panel.grid.minor = ggplot2::element_blank(),
          panel.background = ggplot2::element_blank(),
          panel.grid.major = ggplot2::element_blank(),
          axis.ticks = ggplot2::element_blank(),
          axis.text.y = themeRA,
          axis.text.x = theme,
          axis.title = theme,
          legend.key = ggplot2::element_blank(),
          strip.text.x = theme,
          strip.text.y = theme,
          strip.background = ggplot2::element_blank(),
          legend.position = "none")
  if (!missing(title)) {
    plot <- plot + ggplot2::ggtitle(title)
  }
  if (!is.null(fileName))
    ggplot2::ggsave(fileName, plot, width = 1.6 + 3 * nrow(dd), height = 2.8, dpi = 400)
  return(plot)
}
