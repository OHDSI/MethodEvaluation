MethodEvaluation
================


Introduction
============
This R package contains resources for the evaluation of the performance of methods that aim to estimate the magnitude (relative risk) of the effect of a drug on an outcome. 
These resources include reference sets for evaluating methods on real data, as well as functions for inserting simulated effects in real data based on negative control drug-outcome pairs. Further included are functions for the computation of the minimum detectable relative risks and functions for computing performance statistics such as predictive accuracy, error and bias.

Features
========
- Contains the OMOP and EU-ADR reference set for evaluating method performance using real data.
- Function for inserting simulated effects in real data based on negative control drug-outcome pairs.
- Function for computation of the minimum detectable relative risk (MDRR)
- Functions for computing predictive accuracy, error, and bias

Technology
==========
MethodEvaluation is a pure R package.

Dependencies
============
 * DatabaseConnector
 * SqlRender
 * CohortMethod
 * Cyclops

Getting Started
===============
1. On Windows, make sure [RTools](http://cran.r-project.org/bin/windows/Rtools/) is installed.
2. The DatabaseConnector and SqlRender packages require Java. Java can be downloaded from
<a href="http://www.java.com" target="_blank">http://www.java.com</a>.
3. In R, use the following commands to download and install CohortMethod:

  ```r
  install.packages("devtools")
  library(devtools)
  install_github("ohdsi/SqlRender") 
  install_github("ohdsi/DatabaseConnector") 
  install_github("ohdsi/Cyclops") 
  install_github("ohdsi/CohortMethod") 
  install_github("ohdsi/MethodEvaluation") 
  ```

Getting Involved
=============
* Package manual: [MethodEvaluation.pdf](https://raw.githubusercontent.com/OHDSI/MethodEvaluation/master/man/MethodEvaluation.pdf) 
* Developer questions/comments/feedback: <a href="http://forums.ohdsi.org/c/developers">OHDSI Forum</a>
* We use the <a href="../../issues">GitHub issue tracker</a> for all bugs/issues/enhancements
 
License
=======
MethodEvaluation is licensed under Apache License 2.0

Development
===========
MethodEvaluation is being developed in R Studio.

###Development status
Currently under development. Do not use.
