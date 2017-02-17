# @file CreateOutcomeCohorts.R
#
# Copyright 2017 Observational Health Data Sciences and Informatics
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

#' Create outcomes of interest
#'
#' @details
#' This function will create the outcomes of interest referenced in the various reference sets. The
#' outcomes of interest are derives using information like diagnoses, procedures, and drug
#' prescriptions. The outcomes are stored in a table on the database server.
#'
#' @param connectionDetails      An R object of type \code{ConnectionDetails} created using the
#'                               function \code{createConnectionDetails} in the
#'                               \code{DatabaseConnector} package.
#' @param cdmDatabaseSchema      A database schema containing health care data in the OMOP Commond Data
#'                               Model. Note that for SQL Server, botth the database and schema should
#'                               be specified, e.g. 'cdm_schema.dbo'
#' @param createNewCohortTable   Should a new cohort table be created, or should the outcomes be
#'                               inserted in a existing table?
#' @param cohortDatabaseSchema   The database schema where the target table is located. Note that for
#'                               SQL Server, botth the database and schema should be specified, e.g.
#'                               'cdm_schema.dbo'
#' @param cohortTable            The name of the table where the outcomes will be stored.
#' @param referenceSet           The name of the reference set for which outcomes need to be created.
#'
#' @export
createOutcomeCohorts <- function(connectionDetails,
                                 cdmDatabaseSchema,
                                 createNewCohortTable = FALSE,
                                 cohortDatabaseSchema = cdmDatabaseSchema,
                                 cohortTable = "cohort",
                                 referenceSet = "omopReferenceSet") {
  
  if (referenceSet == "omopReferenceSet") {
    writeLines("Generating HOIs for the OMOP reference set")
    renderedSql <- SqlRender::loadRenderTranslateSql("CreateOmopHois.sql",
                                                     packageName = "MethodEvaluation",
                                                     dbms = connectionDetails$dbms,
                                                     cdm_database_schema = cdmDatabaseSchema,
                                                     create_new_cohort_table = createNewCohortTable,
                                                     cohort_database_schema = cohortDatabaseSchema,
                                                     cohort_table = cohortTable)
  } else if (referenceSet == "euadrReferenceSet") {
    writeLines("Generating HOIs for the EU-ADR reference set")
    # TODO: add code for creating the EU-ADR HOIs
  } else {
    stop(paste("Unknow reference set:", referenceSet))
  }
  conn <- DatabaseConnector::connect(connectionDetails)
  
  writeLines("Executing multiple queries. This could take a while")
  DatabaseConnector::executeSql(conn, renderedSql)
  writeLines("Done")
  dummy <- RJDBC::dbDisconnect(conn)
}
