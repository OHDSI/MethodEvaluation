{DEFAULT @cdm_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_table = 'condition_era' }
{DEFAULT @output_database_schema = 'CDM4_SIM' } 
{DEFAULT @output_table = 'cohort' }
{DEFAULT @create_output_table = TRUE}
{DEFAULT @temp_outcomes_table = '#temp_outcomes'}

{@create_output_table} ? {
IF OBJECT_ID('@output_database_schema.@output_table', 'U') IS NOT NULL
	DROP TABLE @output_database_schema.@output_table;
	
SELECT cohort_definition_id, cohort_start_date, cohort_end_date, subject_id 
INTO @output_database_schema.@output_table 
FROM (
} : {
DELETE FROM @output_database_schema.@output_table 
WHERE cohort_definition_id IN (SELECT new_outcome_id FROM #to_copy);

INSERT INTO @output_database_schema.@output_table (cohort_definition_id, cohort_start_date, cohort_end_date, subject_id)
}
-- Add new outcomes:
SELECT cohort_definition_id, 
	cohort_start_date, 
	NULL AS cohort_end_date, 
	subject_id 
FROM @temp_outcomes_table 

	UNION ALL

-- Copy old outcomes:
{@outcome_table == 'condition_era' } ? {
	SELECT to_copy.new_outcome_id AS cohort_definition_id,
		condition_era_start_date AS cohort_start_date,
		condition_era_end_date AS cohort_end_date,
		person_id AS subject_id  
	FROM @cdm_database_schema.condition_era
	INNER JOIN #to_copy to_copy
	ON condition_concept_id = to_copy.outcome_id
} : {
	SELECT to_copy.new_outcome_id AS cohort_definition_id,
		cohort_start_date,
		cohort_end_date,
		subject_id
	FROM @outcome_database_schema.@outcome_table
	INNER JOIN #to_copy to_copy
	ON cohort_definition_id = to_copy.outcome_id
}

{@create_output_table} ? {
) temp
}
;
