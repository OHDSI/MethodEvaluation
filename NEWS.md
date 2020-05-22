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