# @file MethodEvaluation.R
#
# Copyright 2015 Observational Health Data Sciences and Informatics
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
#'
#' A reference set of 165 drug-outcome pairs where we believe the drug causes the outcome (
#' positive controls) and 234 drug-outcome pairs where we believe the drug does not cause the 
#' outcome (negative controls). The controls involve 4 health outcomes of interest: acute
#' liver injury, acute kidney injury, acute myocardial infarction, and GI bleeding.
#'
#' @docType data
#' @keywords datasets
#' @name omopReferenceSet
#' @usage data(omopReferenceSet)
#' @format A data frame with 399 rows and 10 variables:
#' \describe{
#'   \item{exposureConceptId}{Concept ID identifying the exposure}
#'   \item{exposureConceptName}{Name of the exposure}
#'   \item{outcomeConceptId}{Concept ID identifying the outcome}
#'   \item{outcomeConceptName}{Name of the outcome}
#'   \item{groundTruth}{0 = negative control, 1 = positive control}
#'   \item{indicationConceptId}{Concept Id identifying the (primary) indication of the drug. To be 
#'   used when one wants to nest the analysis within the indication}
#'   \item{indicationConceptName}{Name of the indication}
#'   \item{comparatorDrugConceptId}{Concept ID identifying a comparator drug that can be used as a 
#'   counterfactual}
#'   \item{comparatorDrugConceptName}{Name of the comparator drug}
#'   \item{comparatorType}{How the comparator was selected}
#' }
#' @references
#' Ryan PB, Schuemie MJ, Welebob E, Duke J, Valentine S, Hartzema AG. Defining a reference set to support 
#' methodological research in drug safety. Drug Safety 36 Suppl 1:S33-47, 2013
NULL

#' The EU-ADR reference set
#'
#' A reference set of 43 drug-outcome pairs where we believe the drug causes the outcome (
#' positive controls) and 50 drug-outcome pairs where we believe the drug does not cause the 
#' outcome (negative controls). The controls involve 10 health outcomes of interest. Note that
#' originally, there was an additional positive control (Nimesulide and acute liver injury), but
#' but Nimesulide is not in RxNorm, and is not available in many countries).
#'
#' @docType data
#' @keywords datasets
#' @name euadrReferenceSet
#' @usage data(euadrReferenceSet)
#' @format A data frame with 399 rows and 10 variables:
#' \describe{
#'   \item{exposureConceptId}{Concept ID identifying the exposure}
#'   \item{exposureConceptName}{Name of the exposure}
#'   \item{outcomeConceptId}{Concept ID identifying the outcome}
#'   \item{outcomeConceptName}{Name of the outcome}
#'   \item{groundTruth}{0 = negative control, 1 = positive control}
#'   \item{indicationConceptId}{Concept Id identifying the (primary) indication of the drug. To be 
#'   used when one wants to nest the analysis within the indication}
#'   \item{indicationConceptName}{Name of the indication}
#'   \item{comparatorDrugConceptId}{Concept ID identifying a comparator drug that can be used as a 
#'   counterfactual}
#'   \item{comparatorDrugConceptName}{Name of the comparator drug}
#'   \item{comparatorType}{How the comparator was selected}
#' }
#' @references
#' Coloma PM, Avillach P, Salvo F, Schuemie MJ, Ferrajolo C, Pariente A, Fourrier-Réglat A,
#' Molokhia M, Patadia V, van der Lei J, Sturkenboom M, Trifirò G. A reference standard for 
#' evaluation of methods for drug safety signal detection using electronic healthcare record 
#' databases. Drug Safety 36(1):13-23, 2013
NULL
