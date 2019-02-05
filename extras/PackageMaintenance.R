# @file PackageMaintenance.R
#
# Copyright 2019 Observational Health Data Sciences and Informatics
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


# Format and check code ---------------------------------------------------
OhdsiRTools::formatRFolder()
OhdsiRTools::checkUsagePackage("MethodEvaluation")
OhdsiRTools::updateCopyrightYearFolder()


# Create manual and vignettes ----------------------------------------------
shell("rm extras/MethodEvaluation.pdf")
shell("R CMD Rd2pdf ./ --output=extras/MethodEvaluation.pdf")

rmarkdown::render("vignettes/OhdsiMethodsBenchmark.Rmd",
                  output_file = "../inst/doc/OhdsiMethodsBenchmark.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))

pkgdown::build_site()

# Load reference sets ----------------------------------------------------- OMOP
omopReferenceSet <- read.csv("C:/home/Research/Method evaluation task force/OmopRefSet.csv")
names(omopReferenceSet) <- SqlRender::snakeCaseToCamelCase(names(omopReferenceSet))
save(omopReferenceSet, file = "data/omopReferenceSet.rda", compress = "xz")

# EUADR
euadrReferenceSet <- read.csv("C:/home/Research/Method evaluation task force/EUADRRefSet.csv")
names(euadrReferenceSet) <- SqlRender::snakeCaseToCamelCase(names(euadrReferenceSet))
save(euadrReferenceSet, file = "data/euadrReferenceSet.rda", compress = "xz")

# OHDSI
library(XLConnect)
workbook <- loadWorkbook("C:/home/Research/Method evaluation task force/SearchForNegativeControls/FullSetOfNegativeControls10November2017.xlsx")
sheetNames <- getSheets(workbook)
ohdsiNegativeControls <- data.frame()
for (sheetName in sheetNames) {
  sheet <- readWorksheet(object = workbook,
                         sheet = sheetName,
                         startRow = 0,
                         endRow = 0,
                         startCol = 0,
                         endCol = 0)
  ohdsiNegativeControls <- rbind(ohdsiNegativeControls, sheet)
}
save(ohdsiNegativeControls, file = "data/ohdsiNegativeControls.rda", compress = "xz")
saveRDS(ohdsiNegativeControls, file = "inst/ohdsiNegativeControls.rds")

38000184
OhdsiRTools::insertCohortDefinitionInPackage(152767,
                                             "acute_pancreatitis",
                                             baseUrl = "http://api.ohdsi.org:80/WebAPI")
OhdsiRTools::insertCohortDefinitionInPackage(152768,
                                             "gi_bleed",
                                             baseUrl = "http://api.ohdsi.org:80/WebAPI")
OhdsiRTools::insertCohortDefinitionInPackage(152769,
                                             "ibd",
                                             baseUrl = "http://api.ohdsi.org:80/WebAPI")
OhdsiRTools::insertCohortDefinitionInPackage(152772,
                                             "stroke",
                                             baseUrl = "http://api.ohdsi.org:80/WebAPI")
