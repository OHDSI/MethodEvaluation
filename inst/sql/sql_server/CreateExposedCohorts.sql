/************************************************************************
@file CreateExposedCohorts.sql

Copyright 2023 Observational Health Data Sciences and Informatics

This file is part of MethodEvaluation

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

{DEFAULT @cdm_database_schema = 'CDM4_SIM.dbo'} 
{DEFAULT @exposure_ids = ''} 
{DEFAULT @washout_period = 183} 
{DEFAULT @exposure_database_schema = 'CDM4_SIM'} 
{DEFAULT @exposure_table = 'drug_era'}
{DEFAULT @first_exposure_only = FALSE}
{DEFAULT @risk_window_start = 0}
{DEFAULT @risk_window_end = 0}
{DEFAULT @add_exposure_days_to_end = TRUE}

IF OBJECT_ID('tempdb..#cohort_person', 'U') IS NOT NULL
	DROP TABLE #cohort_person;

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT ROW_NUMBER() OVER (ORDER BY cohort_definition_id, subject_id, cohort_start_date) AS row_id,
    cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date,
	observation_period_end_date,
	era_number
INTO #cohort_person
FROM (
	SELECT 
		cohort_definition_id,
		subject_id,
		DATEADD(DAY, @risk_window_start, cohort_start_date) AS cohort_start_date,
		{@add_exposure_days_to_end} ? {
			CASE WHEN DATEADD(DAY, @risk_window_end, cohort_end_date) > observation_period_end_date THEN observation_period_end_date ELSE DATEADD(DAY, @risk_window_end, cohort_end_date) END AS cohort_end_date,
		} : {
			CASE WHEN DATEADD(DAY, @risk_window_end, cohort_start_date) > observation_period_end_date THEN observation_period_end_date ELSE DATEADD(DAY, @risk_window_end, cohort_start_date) END AS cohort_end_date,
		}
		observation_period_start_date,
		observation_period_end_date,
		era_number
	FROM 
	(
{@exposure_table == 'drug_era' } ? {
	SELECT drug_concept_id AS cohort_definition_id, 
		person_id AS subject_id,
		drug_era_start_date AS cohort_start_date, 
		drug_era_end_date AS cohort_end_date,
		ROW_NUMBER () OVER (PARTITION BY drug_concept_id, person_id ORDER BY drug_era_start_date) AS era_number
	FROM @cdm_database_schema.drug_era 
	WHERE drug_concept_id IN (@exposure_ids)
} : {
	SELECT cohort_definition_id, 
		subject_id,
		cohort_start_date, 
		cohort_end_date,
		ROW_NUMBER () OVER (PARTITION BY cohort_definition_id, subject_id ORDER BY cohort_start_date) AS era_number
	FROM @exposure_database_schema.@exposure_table exposure
	WHERE cohort_definition_id IN (@exposure_ids)
} 
	) exposure
	INNER JOIN @cdm_database_schema.observation_period
		ON observation_period.person_id = exposure.subject_id
		AND cohort_start_date >= observation_period_start_date
		AND cohort_start_date <= observation_period_end_date
) unfiltered
WHERE cohort_start_date <= observation_period_end_date
	AND cohort_start_date <= cohort_end_date
	AND cohort_start_date >= DATEADD(DAY, @washout_period, observation_period_start_date)
{@first_exposure_only} ? {
	AND era_number = 1
}	
;
