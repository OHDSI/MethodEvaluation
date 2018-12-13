MethodEvaluation
================

[![Build Status](https://travis-ci.org/OHDSI/MethodEvaluation.svg?branch=master)](https://travis-ci.org/OHDSI/MethodEvaluation)

MethodEvaluation is part of the [OHDSI Methods Library](https://ohdsi.github.io/MethodsLibrary).

Introduction
============
This R package contains resources for the evaluation of the performance of methods that aim to estimate the magnitude (relative risk) of the effect of a drug on an outcome. 
These resources include reference sets for evaluating methods on real data, as well as functions for inserting simulated effects in real data based on negative control drug-outcome pairs. Further included are functions for the computation of the minimum detectable relative risks and functions for computing performance statistics such as predictive accuracy, error and bias.

Features
========
- Contains the OMOP and EU-ADR reference set, and the OHDSI Method benchmark for evaluating method performance using real data.
- Function for inserting simulated effects in real data based on negative control drug-outcome pairs.
- Function for computation of the minimum detectable relative risk (MDRR)
- Functions for computing predictive accuracy, error, and bias

Technology
==========
MethodEvaluation is a pure R package.

Installation
============
1. On Windows, make sure [RTools](http://cran.r-project.org/bin/windows/Rtools/) is installed.
2. The DatabaseConnector and SqlRender packages require Java. Java can be downloaded from
<a href="http://www.java.com" target="_blank">http://www.java.com</a>.
3. In R, use the following commands to download and install MethodEvaluation:

  ```r
  install.packages("drat")
  drat::addRepo("OHDSI")
  install.packages("MethodEvaluation")
  ```

User Documentation
==================
* Package manual: [MethodEvaluation.pdf](https://raw.githubusercontent.com/OHDSI/MethodEvaluation/master/extras/MethodEvaluation.pdf) 
* Vignette: [Running the OHDSI Methods Benchmark](https://raw.githubusercontent.com/OHDSI/OhdsiMethodsBenchmark/master/inst/doc/OhdsiMethodsBenchmark.pdf)

Support
=======
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="https://github.com/OHDSI/MethodEvaluation/issues">GitHub issue tracker</a> for all bugs/issues/enhancements
 
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
