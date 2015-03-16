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

{DEFAULT @cdm_database = 'CDM4_SIM' } 
{DEFAULT @create_new_cohort_table = TRUE} 
{DEFAULT @cohort_database = 'scratch' } 
{DEFAULT @cohort_database_schema = 'scratch.dbo' } 
{DEFAULT @cohort_table = 'omop_hois' }

USE @cohort_database;

{@create_new_cohort_table} ? {

IF OBJECT_ID('@cohort_table', 'U') IS NOT NULL
	DROP TABLE @cohort_table;

CREATE TABLE @cohort_table (
	cohort_id INT IDENTITY(1, 1) PRIMARY KEY,
	cohort_concept_id INT NOT NULL,
	cohort_start_date DATE NOT NULL,
	cohort_end_date DATE NULL,
	subject_id BIGINT NOT NULL,
	);

}

USE @cdm_database;

-- Acute Liver Injury
-- 1. Broad definition
INSERT INTO @cohort_database_schema.@cohort_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_concept_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000301
FROM condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM vocabulary.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT target_concept_id
				FROM vocabulary.source_to_concept_map
				WHERE primary_map = 'Y'
					AND (
						invalid_reason IS NULL
						OR invalid_reason = ''
						)
					AND source_vocabulary_id = 2
					AND lower(mapping_type) = 'condition'
					AND (
						source_code LIKE '277.4'
						OR source_code LIKE '570%'
						OR source_code LIKE '572.2'
						OR source_code LIKE '572.4%'
						OR source_code LIKE '573%'
						OR source_code LIKE '576.8'
						OR source_code LIKE '782.4'
						OR source_code LIKE '789.1%'
						OR source_code LIKE '790.4%'
						OR source_code LIKE '794.8%'
						)
				)
		);

-- Acute Kidney Injury
-- 1. Broad definition
INSERT INTO @cohort_database_schema.@cohort_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_concept_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000401
FROM condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM vocabulary.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT target_concept_id
				FROM vocabulary.source_to_concept_map
				WHERE primary_map = 'Y'
					AND (
						invalid_reason IS NULL
						OR invalid_reason = ''
						)
					AND source_vocabulary_id = 2
					AND lower(mapping_type) = 'condition'
					AND (source_code LIKE '584%')
				)
		);

-- Acute Myocardial Infarction
-- 1. Broad definition 
INSERT INTO @cohort_database_schema.@cohort_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_concept_id
	)
-- get cases with diagnostic code irrespective of hospitalization
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500000801
FROM condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM vocabulary.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT target_concept_id
				FROM vocabulary.source_to_concept_map
				WHERE primary_map = 'Y'
					AND (
						invalid_reason IS NULL
						OR invalid_reason = ''
						)
					AND source_vocabulary_id = 2
					AND lower(mapping_type) = 'condition'
					AND (
						source_code LIKE '410%'
						OR source_code LIKE '411.1'
						OR source_code LIKE '411.8'
						)
				)
		)

UNION

-- add the angina during hospitalization
SELECT DISTINCT o.person_id,
	o.condition_start_date,
	o.condition_end_date,
	500000801
FROM condition_occurrence o
INNER JOIN visit_occurrence v
	ON v.person_id = o.person_id
		AND o.condition_start_date >= v.visit_start_date
		AND o.condition_start_date <= v.visit_end_date
		AND v.place_of_service_concept_id IN (9203 /* ER */, 9201 /* Inpatient*/)
WHERE o.condition_concept_id IN (
		SELECT descendant_concept_id
		FROM vocabulary.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT target_concept_id
				FROM vocabulary.source_to_concept_map
				WHERE primary_map = 'Y'
					AND (
						invalid_reason IS NULL
						OR invalid_reason = ''
						)
					AND source_vocabulary_id = 2
					AND lower(mapping_type) = 'condition'
					AND source_code = '413.9'
				)
		);

-- GI Bleed
-- 3. Narrow definition and diagnostic procedure without hospitalization
INSERT INTO @cohort_database_schema.@cohort_table (
	subject_id,
	cohort_start_date,
	cohort_end_date,
	cohort_concept_id
	)
SELECT DISTINCT person_id,
	condition_start_date,
	condition_end_date,
	500001003
FROM condition_occurrence
WHERE condition_concept_id IN (
		SELECT descendant_concept_id
		FROM vocabulary.concept_ancestor
		WHERE ancestor_concept_id IN (
				SELECT target_concept_id
				FROM vocabulary.source_to_concept_map
				WHERE primary_map = 'Y'
					AND (
						invalid_reason IS NULL
						OR invalid_reason = ''
						)
					AND source_vocabulary_id = 2
					AND lower(mapping_type) = 'condition'
					AND source_code IN ('534.20', '535.01', '533.20', '533.0', '534.0', '534.2', '532.21', '535.31', '531.31', '533.00', '533.21', '531.21', '532.00', '534.00', '532.2', '531.0', '532.01', '534.31', '535.11', '535.00', '531.20', '531.00', '534.01', '532.0', '534.21', '531.2', '533.2', '533.01', '534.30', '531.01', '532.20', '532.60', '532.61', '534.40', '534.41', '532.41', '533.4', '531.60', '531.40', '531.61', '531.6', '531.4', '534.61', '533.6', '532.6', '532.4', '532.40', '534.4', '534.60', '534.6', '531.41', '578', '456.0', '530.82', '530.21', '530.7', '456.20', '535.71', '772.4', '533.41', '533.40', '578.0', '535.21', '784.8', '533.60', '535.61', '533.61', '578.1')
				)
		);
