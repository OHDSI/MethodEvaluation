# @file MethodEvaluation.R
#
# Copyright 2020 Observational Health Data Sciences and Informatics
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

#' @keywords internal
"_PACKAGE"

#' @importFrom SqlRender loadRenderTranslateSql translateSql
#' @importFrom grDevices rgb
#' @importFrom stats aggregate coef pnorm predict qnorm quantile rexp rpois
#' @importFrom utils write.csv install.packages menu read.csv
#' @importFrom methods is
#' @importFrom rlang .data
#' @import Cyclops
#' @import DatabaseConnector
#' @import FeatureExtraction
#' @import dplyr
NULL


#' The OMOP reference set A reference set of 165 drug-outcome pairs where we believe the drug causes
#' the outcome ( positive controls) and 234 drug-outcome pairs where we believe the drug does not
#' cause the outcome (negative controls). The controls involve 4 health outcomes of interest: acute
#' liver injury, acute kidney injury, acute myocardial infarction, and GI bleeding.
#'
#' @docType data
#' @keywords datasets
#' @name omopReferenceSet
#' @usage
#' data(omopReferenceSet)
#' @format
#' A data frame with 399 rows and 10 variables: \describe{ \item{exposureId}{Concept ID identifying
#' the exposure} \item{exposureName}{Name of the exposure} \item{outcomeId}{Concept ID identifying the
#' outcome} \item{outcomeName}{Name of the outcome} \item{groundTruth}{0 = negative control, 1 =
#' positive control} \item{indicationId}{Concept Id identifying the (primary) indication of the drug.
#' To be used when one wants to nest the analysis within the indication} \item{indicationName}{Name of
#' the indication} \item{comparatorId}{Concept ID identifying a comparator drug that can be used as a
#' counterfactual} \item{comparatorName}{Name of the comparator drug} \item{comparatorType}{How the
#' comparator was selected} }
#' @references
#' Ryan PB, Schuemie MJ, Welebob E, Duke J, Valentine S, Hartzema AG. Defining a reference set to
#' support methodological research in drug safety. Drug Safety 36 Suppl 1:S33-47, 2013
NULL

#' The EU-ADR reference set
#' A reference set of 43 drug-outcome pairs where we believe the drug causes the outcome ( positive
#' controls) and 50 drug-outcome pairs where we believe the drug does not cause the outcome (negative
#' controls). The controls involve 10 health outcomes of interest. Note that originally, there was an
#' additional positive control (Nimesulide and acute liver injury), but Nimesulide is not in RxNorm,
#' and is not available in many countries.
#'
#' @docType data
#' @keywords datasets
#' @name euadrReferenceSet
#' @usage
#' data(euadrReferenceSet)
#' @format
#' A data frame with 399 rows and 10 variables: \describe{ \item{exposureId}{Concept ID identifying
#' the exposure} \item{exposureName}{Name of the exposure} \item{outcomeId}{Concept ID identifying the
#' outcome} \item{outcomeName}{Name of the outcome} \item{groundTruth}{0 = negative control, 1 =
#' positive control} \item{indicationId}{Concept Id identifying the (primary) indication of the drug.
#' To be used when one wants to nest the analysis within the indication} \item{indicationName}{Name of
#' the indication} \item{comparatorId}{Concept ID identifying a comparator drug that can be used as a
#' counterfactual} \item{comparatorName}{Name of the comparator drug} \item{comparatorType}{How the
#' comparator was selected} }
#' @references
#' Coloma PM, Avillach P, Salvo F, Schuemie MJ, Ferrajolo C, Pariente A, Fourrier-Reglat A, Molokhia
#' M, Patadia V, van der Lei J, Sturkenboom M, Trifiro G. A reference standard for evaluation of
#' methods for drug safety signal detection using electronic healthcare record databases. Drug Safety
#' 36(1):13-23, 2013
NULL

#' The OHDSI Method Evaluation Benchmark - Negative Controls
#' A set of 200 negative controls, centered around four outcomes of interest (acute pancreatitis, GI
#' bleeding, Stroke, and IBD), and 4 exposures of interest (diclofenac, ciprofloxacin, metformin, and
#' sertraline), which 25 negative controls each. Each drug-outcome pair also includes a comparator
#' drug (where the comparator is also a negative control), allowing for evaluation of comparative
#' effect estimation, and a nesting cohort for evaluating methods such as the nested case-control
#' design.
#' The exposure, outcome, and nesting cohorts can be created using the
#' \code{\link{createReferenceSetCohorts}} function.
#' These negative controls can form the basis to generate positive controls using the
#' \code{\link{injectSignals}} function.
#'
#' @docType data
#' @keywords datasets
#' @name ohdsiNegativeControls
#' @usage
#' data(ohdsiNegativeControls)
#' @format
#' A data frame with 200 rows and 9 variables: \describe{ \item{targetId}{Cohort ID identifying the
#' target exposure} \item{targetName}{Name of the target cohort} \item{comparatorId}{Cohort ID
#' identifying the comparator exposure} \item{comparatorName}{Name of the comparator cohort}
#' \item{nestingId}{Cohort ID identifying the nesting cohort} \item{nestingName}{Name of the nesting
#' cohort} \item{outcomeId}{Cohort ID identifying the outcome} \item{outcomeName}{Name of the outcome}
#' \item{type}{THe type of control: exposure or outcome}}
NULL
