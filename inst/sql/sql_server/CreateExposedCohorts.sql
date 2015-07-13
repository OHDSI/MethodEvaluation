/************************************************************************
@file GetExposedCohorts.sql

Copyright 2015 Observational Health Data Sciences and Informatics

This file is part of CohortMethod

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
************************************************************************/

{DEFAULT @cdm_database = 'CDM4_SIM'} 
{DEFAULT @exposure_concept_ids = ''} 
{DEFAULT @washout_window = 183} 
{DEFAULT @exposure_database_schema = 'CDM4_SIM'} 
{DEFAULT @exposure_table = 'drug_era'}
{DEFAULT @first_exposure_only = FALSE}
{DEFAULT @risk_window_start = 0}
{DEFAULT @risk_window_end = 0}
{DEFAULT @add_exposure_days_to_end = TRUE}

USE @cdm_database;

IF OBJECT_ID('tempdb..#cohort_person', 'U') IS NOT NULL
	DROP TABLE #cohort_person;

SELECT cohort_concept_id,
	subject_id,
	DATEADD(DAY, @risk_window_start, cohort_start_date) AS cohort_start_date,
    {@add_exposure_days_to_end} ? {
		CASE WHEN DATEADD(DAY, @risk_window_end, cohort_end_date) > observation_period_end_date THEN observation_period_end_date ELSE DATEADD(DAY, @risk_window_end, cohort_end_date) END AS cohort_end_date
	} : {
		CASE WHEN DATEADD(DAY, @risk_window_end, cohort_start_date) > observation_period_end_date THEN observation_period_end_date ELSE DATEADD(DAY, @risk_window_end, cohort_start_date) END AS cohort_end_date
	}
INTO #cohort_person
FROM 
(
{@exposure_table == 'drug_era' } ? {
 
{@first_exposure_only} ? {
SELECT drug_concept_id AS cohort_concept_id, 
	person_id AS subject_id,
	MIN(drug_era_start_date) AS cohort_start_date, 
	MIN(drug_era_end_date) AS cohort_end_date
FROM drug_era 
WHERE drug_concept_id IN (@exposure_concept_ids)
GROUP BY drug_concept_id, 
	person_id
} : {
SELECT drug_concept_id AS cohort_concept_id, 
	person_id AS subject_id,
	drug_era_start_date AS cohort_start_date, 
	drug_era_end_date AS cohort_end_date
FROM drug_era 
WHERE drug_concept_id IN (@exposure_concept_ids)
}
} : {
{@first_exposure_only} ? {
SELECT cohort_concept_id, 
	subject_id,
	MIN(cohort_start) AS cohort_start_date, 
	MIN(cohort_end_date) AS cohort_end_date
FROM @exposure_database_schema.@exposure_table exposure 
WHERE cohort_concept_id IN (@exposure_concept_ids)
GROUP BY cohort_concept_id, 
	subject_id
} : {
SELECT cohort_concept_id, 
	subject_id,
	cohort_start_date, 
	cohort_end_date
FROM @exposure_database_schema.@exposure_table exposure
WHERE cohort_concept_id IN (@exposure_concept_ids)
}
} 
) exposure
INNER JOIN observation_period
	ON observation_period.person_id = exposure.subject_id
		AND cohort_start_date >= DATEADD(DAY, @washout_window, observation_period_start_date)
		AND cohort_end_date <= observation_period_end_date
		AND DATEADD(DAY, @risk_window_start, cohort_start_date) <= observation_period_end_date;