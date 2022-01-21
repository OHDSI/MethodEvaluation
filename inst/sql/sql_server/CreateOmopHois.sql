/************************************************************************
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

{DEFAULT @cdm_database_schema = 'CDM_SIM.dbo' } 
{DEFAULT @outcome_database_schema = 'scratch.dbo' } 
{DEFAULT @outcome_table = 'omop_hois' }

IF OBJECT_ID('@outcome_database_schema.@outcome_table', 'U') IS NOT NULL
	DROP TABLE @outcome_database_schema.@outcome_table;

CREATE TABLE @outcome_database_schema.@outcome_table (
	cohort_definition_id INT NOT NULL,
	cohort_start_date DATE NOT NULL,
	cohort_end_date DATE NULL,
	subject_id BIGINT NOT NULL
	);

-- Acute Liver Injury
-- 1. Broad definition
INSERT INTO @outcome_database_schema.@outcome_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_definition_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000301
FROM @cdm_database_schema.condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT standard.concept_id
				FROM @cdm_database_schema.concept icd9
				INNER JOIN @cdm_database_schema.concept_relationship
					ON concept_id_1 = icd9.concept_id
				INNER JOIN @cdm_database_schema.concept standard
					ON concept_id_2 = standard.concept_id
				WHERE relationship_id = 'Maps to'
					AND icd9.vocabulary_id = 'ICD9CM'
					AND standard.standard_concept = 'S'
					AND standard.domain_id = 'Condition'
					AND (
						icd9.concept_code LIKE '277.4'
						OR icd9.concept_code LIKE '570%'
						OR icd9.concept_code LIKE '572.2'
						OR icd9.concept_code LIKE '572.4%'
						OR icd9.concept_code LIKE '573%'
						OR icd9.concept_code LIKE '576.8'
						OR icd9.concept_code LIKE '782.4'
						OR icd9.concept_code LIKE '789.1%'
						OR icd9.concept_code LIKE '790.4%'
						OR icd9.concept_code LIKE '794.8%'
						)
				)
		);

-- Acute Kidney Injury
-- 1. Broad definition
INSERT INTO @outcome_database_schema.@outcome_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_definition_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000401
FROM @cdm_database_schema.condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT standard.concept_id
				FROM @cdm_database_schema.concept icd9
				INNER JOIN @cdm_database_schema.concept_relationship
					ON concept_id_1 = icd9.concept_id
				INNER JOIN @cdm_database_schema.concept standard
					ON concept_id_2 = standard.concept_id
				WHERE relationship_id = 'Maps to'
					AND icd9.vocabulary_id = 'ICD9CM'
					AND standard.standard_concept = 'S'
					AND standard.domain_id = 'Condition'
					AND icd9.concept_code LIKE '584%'
				)
		);

-- Acute Myocardial Infarction
-- 1. Broad definition 
INSERT INTO @outcome_database_schema.@outcome_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_definition_id
	)
-- get cases with diagnostic code irrespective of hospitalization
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000801
FROM @cdm_database_schema.condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT standard.concept_id
				FROM @cdm_database_schema.concept icd9
				INNER JOIN @cdm_database_schema.concept_relationship
					ON concept_id_1 = icd9.concept_id
				INNER JOIN @cdm_database_schema.concept standard
					ON concept_id_2 = standard.concept_id
				WHERE relationship_id = 'Maps to'
					AND icd9.vocabulary_id = 'ICD9CM'
					AND standard.standard_concept = 'S'
					AND standard.domain_id = 'Condition'
					AND (
						icd9.concept_code LIKE '410%'
						OR icd9.concept_code LIKE '411.1'
						OR icd9.concept_code LIKE '411.8'
						)
				)
		)

UNION

-- add the angina during hospitalization
SELECT DISTINCT o.person_id,
	o.condition_start_date,
	o.condition_end_date,
	500000801
FROM @cdm_database_schema.condition_occurrence o
INNER JOIN @cdm_database_schema.visit_occurrence v
	ON v.person_id = o.person_id
		AND o.condition_start_date >= v.visit_start_date
		AND o.condition_start_date <= v.visit_end_date
		AND v.visit_concept_id IN (9203 /* ER */, 9201 /* Inpatient*/)
WHERE o.condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT standard.concept_id
				FROM @cdm_database_schema.concept icd9
				INNER JOIN @cdm_database_schema.concept_relationship
					ON concept_id_1 = icd9.concept_id
				INNER JOIN @cdm_database_schema.concept standard
					ON concept_id_2 = standard.concept_id
				WHERE relationship_id = 'Maps to'
					AND icd9.vocabulary_id = 'ICD9CM'
					AND standard.standard_concept = 'S'
					AND standard.domain_id = 'Condition'
					AND icd9.concept_code = '413.9'
				)
		);


-- GI Bleed
-- 3. Narrow definition and diagnostic procedure without hospitalization
INSERT INTO @outcome_database_schema.@outcome_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_definition_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500001003
FROM @cdm_database_schema.condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM @cdm_database_schema.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT standard.concept_id
				FROM @cdm_database_schema.concept icd9
				INNER JOIN @cdm_database_schema.concept_relationship
					ON concept_id_1 = icd9.concept_id
				INNER JOIN @cdm_database_schema.concept standard
					ON concept_id_2 = standard.concept_id
				WHERE relationship_id = 'Maps to'
					AND icd9.vocabulary_id = 'ICD9CM'
					AND standard.standard_concept = 'S'
					AND standard.domain_id = 'Condition'
					AND icd9.concept_code IN ('534.20', '535.01', '533.20', '533.0', '534.0', '534.2', '532.21', '535.31', '531.31', '533.00', '533.21', '531.21', '532.00', '534.00', '532.2', '531.0', '532.01', '534.31', '535.11', '535.00', '531.20', '531.00', '534.01', '532.0', '534.21', '531.2', '533.2', '533.01', '534.30', '531.01', '532.20', '532.60', '532.61', '534.40', '534.41', '532.41', '533.4', '531.60', '531.40', '531.61', '531.6', '531.4', '534.61', '533.6', '532.6', '532.4', '532.40', '534.4', '534.60', '534.6', '531.41', '578', '456.0', '530.82', '530.21', '530.7', '456.20', '535.71', '772.4', '533.41', '533.40', '578.0', '535.21', '784.8', '533.60', '535.61', '533.61', '578.1')
				)
		);

