/************************************************************************
@file GetOutcomes.sql

Copyright 2022 Observational Health Data Sciences and Informatics

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

{DEFAULT @cdm_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_table = 'condition_occurrence' }
{DEFAULT @first_outcome_only = TRUE }

SELECT row_id,
	outcome_id,
	SUM(within_observation) AS y_itt,
	SUM(within_cohort) AS y,
	MIN(time_to_event) AS time_to_event
FROM (
	SELECT DISTINCT	exposure.row_id,
		outcome.outcome_id,
		CASE 
			WHEN outcome_date <= exposure.cohort_end_date THEN 1
			ELSE 0
		END AS within_cohort,
		1 AS within_observation,
		DATEDIFF(DAY, exposure.cohort_start_date, outcome_date) AS time_to_event
	FROM #cohort_person exposure
	INNER JOIN #exposure_outcome exposure_outcome
	ON exposure_outcome.exposure_id = exposure.cohort_definition_id
	INNER JOIN (
{@first_outcome_only} ? {
{@outcome_table == 'condition_era' } ? {
		SELECT condition_concept_id AS outcome_id,
		  person_id,
		  MIN(condition_era_start_date) AS outcome_date
		FROM @cdm_database_schema.condition_era
		GROUP BY condition_concept_id,
			person_id
} : {
		SELECT cohort_definition_id AS outcome_id,
		  subject_id AS person_id,
		  MIN(cohort_start_date) AS outcome_date
		FROM @outcome_database_schema.@outcome_table co1
		GROUP BY cohort_definition_id,
			subject_id
}
} : {
{@outcome_table == 'condition_era' } ? {
		SELECT condition_concept_id AS outcome_id,
		  person_id,
		  condition_era_start_date AS outcome_date
		FROM @cdm_database_schema.condition_era
} : {
		SELECT cohort_definition_id AS outcome_id,
		  subject_id AS person_id,
		  cohort_start_date AS outcome_date
		FROM @outcome_database_schema.@outcome_table co1
}
}
		) outcome
	ON outcome.person_id = exposure.subject_id
		AND outcome_date >= exposure.cohort_start_date
		AND outcome_date <= exposure.observation_period_end_date
		AND outcome.outcome_id = exposure_outcome.outcome_id
	) tmp
GROUP BY row_id,
	outcome_id;
