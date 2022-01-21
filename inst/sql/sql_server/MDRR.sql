/************************************************************************
@file MDRR.sql

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

{DEFAULT @exposures_of_interest = 1,2,3}
{DEFAULT @outcomes_of_interest = 1,2,3}
{DEFAULT @cdm_database_schema = 'cdm4_sim'}
{DEFAULT @exposure_database_schema = 'cdm4_sim'}
{DEFAULT @exposure_table = 'drug_era'}
{DEFAULT @exposure_start_date = 'drug_era_start_date'} 
{DEFAULT @exposure_end_date = 'drug_era_end_date'} 
{DEFAULT @exposure_concept_id = 'drug_concept_id'} 
{DEFAULT @exposure_person_id = 'person_id'} 
{DEFAULT @outcome_database_schema = 'cdm4_sim'}
{DEFAULT @outcome_table = 'condition_era'}
{DEFAULT @outcome_start_date = 'condition_era_start_date'} 
{DEFAULT @outcome_end_date = 'condition_era_end_date'} 
{DEFAULT @outcome_concept_id = 'condition_concept_id'} 
{DEFAULT @outcome_person_id = 'person_id'} 

SELECT @exposure_concept_id AS drug_concept_id,
	FLOOR((YEAR(@exposure_start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	COUNT(person.person_id) AS person_count
INTO #drug_prev_count
FROM (
	SELECT @exposure_person_id AS person_id,
		@exposure_concept_id,
		MIN(@exposure_start_date) AS @exposure_start_date
	FROM @exposure_database_schema.@exposure_table
	WHERE @exposure_concept_id IN (@exposures_of_interest)
	GROUP BY @exposure_person_id,
		@exposure_concept_id
	) persons_with_drug
INNER JOIN @cdm_database_schema.person
	ON persons_with_drug.person_id = person.person_id
GROUP BY @exposure_concept_id,
	FLOOR((YEAR(@exposure_start_date) - year_of_birth) / 10),
	gender_concept_id;

SELECT @outcome_concept_id AS condition_concept_id,
	FLOOR((YEAR(@outcome_start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	COUNT(person.person_id) AS person_count
INTO #condition_prev_count
FROM (
	SELECT @outcome_person_id AS person_id,
		@outcome_concept_id,
		MIN(@outcome_start_date) AS @outcome_start_date
	FROM @outcome_database_schema.@outcome_table
	WHERE @outcome_concept_id IN (@outcomes_of_interest)
  {@outcome_condition_type_concept_ids != '' & @outcome_table == 'condition_occurrence'} ? {AND condition_type_concept_id IN (@outcome_condition_type_concept_ids)}
	GROUP BY @outcome_person_id,
		@outcome_concept_id
	) persons_with_condition
INNER JOIN @cdm_database_schema.person
	ON persons_with_condition.person_id = person.person_id
GROUP BY @outcome_concept_id,
	FLOOR((YEAR(@outcome_start_date) - year_of_birth) / 10),
	gender_concept_id;

SELECT FLOOR((YEAR(observation_period_start_date) - year_of_birth) / 10) AS age_group,
	gender_concept_id,
	COUNT(person.person_id) AS person_count
INTO #prev_count
FROM (
	SELECT person_id,
		min(observation_period_start_date) AS observation_period_start_date
	FROM @cdm_database_schema.observation_period
	GROUP BY person_id
	) observation_period
INNER JOIN @cdm_database_schema.person
	ON observation_period.person_id = person.person_id
GROUP BY FLOOR((YEAR(observation_period_start_date) - year_of_birth) / 10),
	gender_concept_id;

SELECT drug_concept_id,
	condition_concept_id,
	SUM(drug_person_count) AS drug_person_count,
	SUM(condition_person_count) AS condition_person_count,
	SUM(person_count) AS person_count,
	SUM(expected_count) AS expected_count,
	CASE 
		WHEN SUM(expected_count) > 0.01
			THEN POWER(1 + ((1.96 + 0.842) / (2 * SQRT(SUM(expected_count)))), 2)
		ELSE POWER(1 + ((1.96 + 0.842) / (2 * SQRT(0.01))), 2)
		END AS mdrr
INTO #mdrr
FROM (
	SELECT dc.drug_concept_id,
		cc.condition_concept_id,
		cc.gender_concept_id,
		cc.age_group,
		dc.person_count AS drug_person_count,
		cc.person_count AS condition_person_count,
		pc.person_count AS person_count,
		dc.person_count * (cc.person_count / CAST(pc.person_count AS FLOAT)) AS expected_count
	FROM #condition_prev_count cc
	INNER JOIN #prev_count pc
		ON cc.age_group = pc.age_group
			AND cc.gender_concept_id = pc.gender_concept_id
	INNER JOIN #drug_prev_count dc
		ON cc.age_group = dc.age_group
			AND cc.gender_concept_id = dc.gender_concept_id
	) per_age_sex
GROUP BY drug_concept_id,
  condition_concept_id;

TRUNCATE TABLE #drug_prev_count;
DROP TABLE #drug_prev_count;

TRUNCATE TABLE #condition_prev_count;
DROP TABLE #condition_prev_count;

TRUNCATE TABLE #prev_count;
DROP TABLE #prev_count;
