# Copyright 2023 Observational Health Data Sciences and Informatics
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
styler::style_pkg()
OhdsiRTools::checkUsagePackage("MethodEvaluation")
OhdsiRTools::updateCopyrightYearFolder()
devtools::spell_check()

# Create manual and vignettes ----------------------------------------------
unlink("extras/MethodEvaluation.pdf")
shell("R CMD Rd2pdf ./ --output=extras/MethodEvaluation.pdf")

rmarkdown::render("vignettes/OhdsiMethodsBenchmark.Rmd",
                  output_file = "../inst/doc/OhdsiMethodsBenchmark.pdf",
                  rmarkdown::pdf_document(latex_engine = "pdflatex",
                                          toc = TRUE,
                                          number_sections = TRUE))
unlink("inst/doc/OhdsiMethodsBenchmark.tex")

pkgdown::build_site()
OhdsiRTools::fixHadesLogo()

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
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))
OhdsiRTools::insertCohortDefinitionInPackage(152768,
                                             "gi_bleed",
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))
OhdsiRTools::insertCohortDefinitionInPackage(152769,
                                             "ibd",
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))
OhdsiRTools::insertCohortDefinitionInPackage(152772,
                                             "stroke",
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))

# OHDSI development
ncs <- read.csv("../Legend/inst/settings/NegativeControls.csv", stringsAsFactors = FALSE)
ncs <- ncs[ncs$indicationId == "Hypertension", ]
ohdsiDevelopmentNegativeControls <- data.frame(targetId = 1,
                                               targetName = "ACE inhibitors",
                                               comparatorId = 2,
                                               comparatorName = "Thiazides or thiazide-like diuretics",
                                               nestingId = 316866,
                                               nestingName = "Hypertension",
                                               outcomeId = ncs$conceptId,
                                               outcomeName = ncs$name,
                                               targetConceptIds = "1335471;1340128;1341927;1363749;1308216;1310756;1373225;1331235;1334456;1342439",
                                               comparatorConceptIds = "1395058;974166;978555;907013",
                                               type = "Outcome control")
save(ohdsiDevelopmentNegativeControls, file = "data/ohdsiDevelopmentNegativeControls.rda", compress = "xz")
saveRDS(ohdsiDevelopmentNegativeControls, file = "inst/ohdsiDevelopmentNegativeControls.rds")
OhdsiRTools::insertCohortDefinitionInPackage(1775839,
                                             "ace_inhibitors",
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))
OhdsiRTools::insertCohortDefinitionInPackage(1775840,
                                             "thiazides_diuretics",
                                             baseUrl = keyring::key_get("ohdsiBaseUrl"))

# Regenerate SQL for cohort definitions ----------------------------------------
# Old Circe SQL is outdated? Doesn't run on DataBricks

cohorts <- list.files("inst/cohorts")
for (cohort in cohorts) {
  json <- readLines(file.path("inst/cohorts", cohort))
  sql <- CirceR::buildCohortQuery(json, options = CirceR::createGenerateOptions(generateStats = FALSE))
  SqlRender::writeSql(sql, file.path("inst/sql/sql_server", gsub(".json", ".sql", cohort)))  
}
