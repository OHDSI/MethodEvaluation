% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/MethodEvaluation.R
\docType{data}
\name{ohdsiDevelopmentNegativeControls}
\alias{ohdsiDevelopmentNegativeControls}
\title{The OHDSI Development Set - Negative Controls
A set of 76 negative control outcomes, all for the exposures of ACE inhibitors (compared to
thiazides and thiazide-like diuretics). This set is a much small set than the he OHDSI Method
Evaluation Benchmark, but follows the same principles. It is intended to be used when developing
methods, leaving the Methods Benchark untouched until a final evaluation of the method, thus preventing
'training' on the evaluation set. The negative controls are borrowed from the LEGEND Hypertension study.
The exposure, outcome, and nesting cohorts can be created using the
\code{\link{createReferenceSetCohorts}} function.
These negative controls can form the basis to generate positive controls using the
\code{\link{injectSignals}} function.}
\format{
A data frame with 76 rows and 11 variables: \describe{ \item{targetId}{Cohort ID identifying the
target exposure} \item{targetName}{Name of the target cohort} \item{comparatorId}{Cohort ID
identifying the comparator exposure} \item{comparatorName}{Name of the comparator cohort}
\item{nestingId}{Cohort ID identifying the nesting cohort} \item{nestingName}{Name of the nesting
cohort} \item{outcomeId}{Cohort ID identifying the outcome} \item{outcomeName}{Name of the outcome}
\item{type}{THe type of control: exposure or outcome} \item{targetConceptIds}{A semi-colon separated list of concept IDs
that together form the target exposure definitions.} \item{comparatorConceptIds}{A semi-colon separated list of concept IDs
that together form the comparator exposure definitions.}}
}
\usage{
data(ohdsiDevelopmentNegativeControls)
}
\description{
The OHDSI Development Set - Negative Controls
A set of 76 negative control outcomes, all for the exposures of ACE inhibitors (compared to
thiazides and thiazide-like diuretics). This set is a much small set than the he OHDSI Method
Evaluation Benchmark, but follows the same principles. It is intended to be used when developing
methods, leaving the Methods Benchark untouched until a final evaluation of the method, thus preventing
'training' on the evaluation set. The negative controls are borrowed from the LEGEND Hypertension study.
The exposure, outcome, and nesting cohorts can be created using the
\code{\link{createReferenceSetCohorts}} function.
These negative controls can form the basis to generate positive controls using the
\code{\link{injectSignals}} function.
}
\keyword{datasets}
