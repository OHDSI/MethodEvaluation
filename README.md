MethodEvaluation
================

[![Build Status](https://github.com/OHDSI/MethodEvaluation/workflows/R-CMD-check/badge.svg)](https://github.com/OHDSI/MethodEvaluation/actions?query=workflow%3AR-CMD-check)

MethodEvaluation is part of [HADES](https://ohdsi.github.io/Hades).

Introduction
============
This R package contains resources for the evaluation of the performance of methods that aim to estimate the magnitude (relative risk) of the effect of a drug on an outcome. 
These resources include reference sets for evaluating methods on real data, as well as functions for inserting simulated effects in real data based on negative control drug-outcome pairs. Further included are functions for the computation of the minimum detectable relative risks and functions for computing performance statistics such as predictive accuracy, error and bias.

Features
========
- Contains the OMOP and EU-ADR reference sets, and the OHDSI Method Benchmark and OHDSI Development Set for evaluating method performance using real data.
- Function for inserting simulated effects in real data based on negative control drug-outcome pairs.
- Function for computation of the minimum detectable relative risk (MDRR)
- Functions for computing predictive accuracy, error, and bias

Technology
==========
MethodEvaluation is a pure R package. 

System Requirements
===================
Requires R. Some of the packages used by MethodEvaluation require Java.

Installation
============

1. See the instructions [here](https://ohdsi.github.io/Hades/rSetup.html) for configuring your R environment, including Java.

2. In R, use the following commands to download and install MethodEvaluation:

  ```r
  install.packages("drat")
  drat::addRepo("OHDSI")
  install.packages("MethodEvaluation")
  ```

User Documentation
==================
Documentation can be found on the [package website](https://ohdsi.github.io/MethodEvaluation).

PDF versions of the documentation are also available:

* Package manual: [MethodEvaluation.pdf](https://raw.githubusercontent.com/OHDSI/MethodEvaluation/master/extras/MethodEvaluation.pdf) 
* Vignette: [Running the OHDSI Methods Benchmark](https://raw.githubusercontent.com/OHDSI/MethodEvaluation/master/inst/doc/OhdsiMethodsBenchmark.pdf)

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/MethodEvaluation/issues">GitHub issue tracker</a> for all bugs/issues/enhancements
 
Contributing
============
Read [here](https://ohdsi.github.io/Hades/contribute.html) how you can contribute to this package.

License
=======
MethodEvaluation is licensed under Apache License 2.0

Development
===========
MethodEvaluation is being developed in R Studio.

### Development status

Beta

Acknowledgements
================
- This project is supported in part through the National Science Foundation grant IIS 1251151.
