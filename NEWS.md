MethodEvaluation 2.4.0
======================

Changes:

1. Updating Circe-generated SQL (to avoid errors on DataBricks).

2. Refactoring code so connections do not stay open longer than strictly necessary (to avoid timeout errors on DataBricks).

3. Increasing required version of FeatureExtraction to avoid warning about use of deprecated argument `oracleTempSchema`.

4. Added `riskWindowStart` argument to `synthesizeReferenceSetPositiveControls()` for rare occasions when we want to have a specific time-at-risk.


Bugfixes:

1. Fixing nesting cohorts to end at end of observation.

2. Fixing calibration when using exposure controls.

3. Fixing error when computing metrics for negative (or positive) controls only.

4. Using character instead of numeric to store person IDs in R to avoid loss of precision when inserting generated outcomes.

5. Always casting dates to dates before operations to avoid hidden casts to date time.


MethodEvaluation 2.3.0
======================

Changes:

1. Using `checkmate` to check function input.

2. Adding `packageCustomBenchmarkResults()` to support custom methods benchmarks.

3. Fixing seeds and setting `resetCoefficients = TRUE` to ensure reproducibility of positive control synthesis.


MethodEvaluation 2.2.0
======================

Changes:

1. Updating from `oracleTempSchema` to `tempEmulationSchema` for newer versions of SqlRender.

2. Added unit tests.


MethodEvaluation 2.1.0
======================

Changes:

1. Added 'ohdsiDevelopment' reference set, intended to be used when developing methods. This set is based on 76 negative controls from the LEGEND Hypertension study.

2. Now generating cohort counts when generating reference set cohorts.

Bugfixes:

1. Fixed error during positive control synthesis for platforms using `oracleTempSchema`.

2. Adding initial support for 64-bit integer covariate and cohort IDs.Still requires [this issue](https://github.com/truecluster/bit64/issues/8) to be resolved.


MethodEvaluation 2.0.0
======================

Changes:

1. Dropping orphan concept and source concept functions, as these are now implemented much better in the CohortDiagnostics packages.

2. Replacing ff with Andromeda for storage of large data objects.


MethodEvaluation 1.1.1
======================

Bugfixes: 

1. Throw meaningful error when there are not enough negative controls with sufficient outcome count to perform positive control synthesis.


MethodEvaluation 1.1.0
======================

Changes:

1. Renamed synthesizePositiveControls to synthesizeReferenceSetPositiveControls, injectSignals to synthesizePositiveControls, and changed addExposureDaysToEnd to endAnchor.

2. Dropping individual metric function. The computeMetrics function now computes the standard set of metrics.

3. Added checkCohortSourceCodes and findOrphanSourceCodes functions. 

MethodEvaluation 1.0.2
======================

Bugfixes:

1. Fixed bug in positive control synthesis causing error when setting riskWindowStart to value other than 0.

MethodEvaluation 1.0.1
======================

Bugfixes:

1. Fixed bug in positive control synthesis causing only most predictive covariates to be used when predicting outcome probabilities.