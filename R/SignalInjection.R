# @file SignalInjection.R
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

#' Inject signals in database
#'
#' @details
#' DEPRECATED. Use \code{\link{synthesizePositiveControls}} instead.
#'
#' @param connectionDetails               An R object of type \code{ConnectionDetails} created using
#'                                        the function \code{createConnectionDetails} in the
#'                                        \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema               Name of database schema that contains OMOP CDM and
#'                                        vocabulary.
#' @param oracleTempSchema                For Oracle only: the name of the database schema where you
#'                                        want all temporary tables to be managed. Requires
#'                                        create/insert permissions to this database.
#' @param exposureDatabaseSchema          The name of the database schema that is the location where
#'                                        the exposure data used to define the exposure cohorts is
#'                                        available.  If exposureTable = DRUG_ERA,
#'                                        exposureDatabaseSchema is not used by assumed to be
#'                                        cdmSchema.  Requires read permissions to this database.
#' @param exposureTable                   The table name that contains the exposure cohorts.  If
#'                                        exposureTable <> DRUG_ERA, then expectation is exposureTable
#'                                        has format of COHORT table: cohort_concept_id, SUBJECT_ID,
#'                                        COHORT_START_DATE, COHORT_END_DATE.
#' @param outcomeDatabaseSchema           The name of the database schema that is the location where
#'                                        the data used to define the outcome cohorts is available. If
#'                                        exposureTable = CONDITION_ERA, exposureDatabaseSchema is not
#'                                        used by assumed to be cdmSchema.  Requires read permissions
#'                                        to this database.
#' @param outcomeTable                    The table name that contains the outcome cohorts. When the
#'                                        table name is not CONDITION_ERA This table is expected to
#'                                        have the same format as the COHORT table: SUBJECT_ID,
#'                                        COHORT_START_DATE, COHORT_END_DATE, COHORT_CONCEPT_ID (CDM
#'                                        v4) or COHORT_DEFINITION_ID (CDM v5 and higher).
#' @param createOutputTable               Should the output table be created prior to inserting the
#'                                        outcomes? If TRUE and the tables already exists, it will
#'                                        first be deleted. If FALSE, the table is assumed to exist and
#'                                        the outcomes will be inserted. Any existing outcomes with the
#'                                        same IDs will first be deleted.
#' @param outputIdOffset                  What should be the first new outcome ID that is to be
#'                                        created?
#' @param exposureOutcomePairs            A data frame with at least two columns:
#'                                        \itemize{
#'                                          \item {"exposureId" containing the drug_concept_ID or
#'                                                cohort_concept_id of the exposure variable}
#'                                          \item {"outcomeId" containing the condition_concept_ID or
#'                                                cohort_concept_id of the outcome variable}
#'                                        }
#'
#'
#' @param modelType                       Can be either "poisson" or "survival"
#' @param minOutcomeCountForModel         Minimum number of outcome events required to build a model.
#' @param minOutcomeCountForInjection     Minimum number of outcome events required to inject a signal.
#' @param covariateSettings               An object of type \code{covariateSettings} as created using
#'                                        the \code{createCovariateSettings} function in the
#'                                        \code{FeatureExtraction} package.
#' @param prior                           The prior used to fit the outcome model. See
#'                                        \code{\link[Cyclops]{createPrior}} for details.
#' @param control                         The control object used to control the cross-validation used
#'                                        to determine the hyperparameters of the prior (if
#'                                        applicable). See \code{\link[Cyclops]{createControl}} for
#'                                        details.
#' @param precision                       The allowed ratio between target and injected signal size.
#' @param firstExposureOnly               Should signals be injected only for the first exposure? (ie.
#'                                        assuming an acute effect)
#' @param washoutPeriod                   Number of days at the start of observation for which no
#'                                        signals will be injected, but will be used to determine
#'                                        whether exposure or outcome is the first one, and for
#'                                        extracting covariates to build the outcome model.
#' @param riskWindowStart                 The start of the risk window relative to the start of the
#'                                        exposure (in days). When 0, risk is assumed to start on the
#'                                        first day of exposure.
#' @param riskWindowEnd                   The end of the risk window relative to the start of the
#'                                        exposure. Note that typically the length of exposure is added
#'                                        to this number (when the \code{addExposureDaysToEnd}
#'                                        parameter is set to TRUE).
#' @param addExposureDaysToEnd            Should length of exposure be added to the risk window?
#' @param addIntentToTreat                If true, the signal will not only be injected in the primary
#'                                        time at risk, but also after the time at risk (up until the
#'                                        obseration period end). In both time periods, the target
#'                                        effect size will be enforced. This allows the same positive
#'                                        control synthesis to be used in both on treatment and
#'                                        intent-to-treat analysis variants. However, this will
#'                                        preclude the controls to be used in self-controlled designs
#'                                        that consider the time after exposure. Requires
#'                                        \code{firstExposureOnly = TRUE}.
#' @param firstOutcomeOnly                Should only the first outcome per person be considered when
#'                                        modeling the outcome?
#' @param removePeopleWithPriorOutcomes   Remove people with prior outcomes?
#' @param maxSubjectsForModel             Maximum number of people used to fit an outcome model.
#' @param effectSizes                     A numeric vector of effect sizes that should be inserted.
#' @param outputDatabaseSchema            The name of the database schema that is the location of the
#'                                        tables containing the new outcomesRequires write permissions
#'                                        to this database.
#' @param outputTable                     The name of the table names that will contain the generated
#'                                        outcome cohorts.
#' @param workFolder                      Path to a folder where intermediate data will be stored.
#' @param cdmVersion                      Define the OMOP CDM version used: currently support "4" and
#'                                        "5".
#' @param modelThreads                    Number of parallel threads to use when fitting outcome
#'                                        models.
#' @param generationThreads               Number of parallel threads to use when generating outcomes.
#'
#' A data.frame listing all the drug-pairs in combination with requested effect sizes and the real
#' inserted effect size (might be different from the requested effect size because of sampling error).
#'
#' @export
injectSignals <- function(connectionDetails,
                          cdmDatabaseSchema,
                          oracleTempSchema = cdmDatabaseSchema,
                          exposureDatabaseSchema = cdmDatabaseSchema,
                          exposureTable = "drug_era",
                          outcomeDatabaseSchema = cdmDatabaseSchema,
                          outcomeTable = "cohort",
                          outputDatabaseSchema = outcomeDatabaseSchema,
                          outputTable = outcomeTable,
                          createOutputTable = FALSE,
                          exposureOutcomePairs,
                          modelType = "poisson",
                          minOutcomeCountForModel = 100,
                          minOutcomeCountForInjection = 25,
                          covariateSettings = FeatureExtraction::createCovariateSettings(useDemographicsAgeGroup = TRUE,
                                                                                         useDemographicsGender = TRUE,
                                                                                         useDemographicsIndexYear = TRUE,
                                                                                         useDemographicsIndexMonth = TRUE,
                                                                                         useConditionGroupEraLongTerm = TRUE,
                                                                                         useDrugGroupEraLongTerm = TRUE,
                                                                                         useProcedureOccurrenceLongTerm = TRUE,
                                                                                         useMeasurementLongTerm = TRUE,
                                                                                         useObservationLongTerm = TRUE,
                                                                                         useCharlsonIndex = TRUE,
                                                                                         useDcsi = TRUE,
                                                                                         useChads2Vasc = TRUE,
                                                                                         longTermStartDays = 365,
                                                                                         endDays = 0),
                          prior = Cyclops::createPrior("laplace", exclude = 0, useCrossValidation = TRUE),
                          control = Cyclops::createControl(cvType = "auto",
                                                           startingVariance = 0.1,
                                                           noiseLevel = "quiet",
                                                           threads = 10),
                          firstExposureOnly = FALSE,
                          washoutPeriod = 183,
                          riskWindowStart = 0,
                          riskWindowEnd = 0,
                          addExposureDaysToEnd = TRUE,
                          addIntentToTreat = FALSE,
                          firstOutcomeOnly = FALSE,
                          removePeopleWithPriorOutcomes = FALSE,
                          maxSubjectsForModel = 1e+05,
                          effectSizes = c(1, 1.25, 1.5, 2, 4),
                          precision = 0.01,
                          outputIdOffset = 1000,
                          workFolder = "./SignalInjectionTemp",
                          cdmVersion = "5",
                          modelThreads = 1,
                          generationThreads = 1) {
  .Deprecated("synhtesizePositiveControls")
  
  if (addExposureDaysToEnd) {
    endAnchor <- "cohort end"
  } else {
    endAnchor <- "cohort start"
  }
  
  synthesizePositiveControls(connectionDetails = connectionDetails,
                             cdmDatabaseSchema = cdmDatabaseSchema,
                             oracleTempSchema = oracleTempSchema,
                             exposureDatabaseSchema = exposureDatabaseSchema,
                             exposureTable = exposureTable,
                             outcomeDatabaseSchema = outcomeDatabaseSchema,
                             outcomeTable = outcomeTable,
                             outputDatabaseSchema = outputDatabaseSchema,
                             outputTable = outputTable,
                             createOutputTable = createOutputTable,
                             exposureOutcomePairs = exposureOutcomePairs,
                             modelType = modelType,
                             minOutcomeCountForModel = minOutcomeCountForModel,
                             minOutcomeCountForInjection = minOutcomeCountForInjection,
                             covariateSettings = covariateSettings,
                             prior = prior,
                             control = control,
                             firstExposureOnly = firstExposureOnly,
                             washoutPeriod = washoutPeriod,
                             riskWindowStart = riskWindowStart,
                             riskWindowEnd = riskWindowEnd,
                             endAnchor = endAnchor,
                             addIntentToTreat = addIntentToTreat,
                             firstOutcomeOnly = firstOutcomeOnly,
                             removePeopleWithPriorOutcomes = removePeopleWithPriorOutcomes,
                             maxSubjectsForModel = maxSubjectsForModel,
                             effectSizes = effectSizes,
                             precision = precision,
                             outputIdOffset = outputIdOffset,
                             workFolder = workFolder,
                             cdmVersion = cdmVersion,
                             modelThreads = modelThreads,
                             generationThreads = generationThreads) 
}
