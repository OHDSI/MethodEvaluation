{DEFAULT @cdm_database = 'CDM4_SIM' } 
{DEFAULT @outcome_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_table = 'condition_occurrence' }
{DEFAULT @outcome_condition_type_concept_ids = '' }
{DEFAULT @output_database_schema = 'scratch.dbo' } 
{DEFAULT @output_table = 'inserted_outcomes' }
{DEFAULT @source_concept_id = 0}
{DEFAULT @target_concept_id = 0}

USE @cdm_database; 

INSERT INTO @output_database_schema.@output_table (cohort_concept_id, cohort_start_date, cohort_end_date, subject_id)
{@outcome_table == 'condition_occurrence' } ? {
	SELECT @target_concept_id AS outcome_concept_id,
		condition_start_date AS cohort_start_date,
		condition_end_date AS cohort_end_date,
		person_id AS subject_id
	FROM condition_occurrence
	WHERE condition_concept_id = @source_concept_id
	  {@outcome_condition_type_concept_ids} ? {AND condition_type_concept_id IN (@outcome_condition_type_concept_ids}
} : { {@outcome_table == 'condition_era' } ? {
	SELECT @target_concept_id AS outcome_concept_id,
		condition_era_start_date AS cohort_start_date,
		condition_era_end_date AS cohort_end_date,
		person_id AS subject_id  
	FROM condition_era
	WHERE condition_concept_id = @source_concept_id
} : {
	SELECT @target_concept_id AS outcome_concept_id,
		cohort_start_date,
		cohort_end_date,
		subject_id
	FROM @outcome_database_schema.@outcome_table
	WHERE cohort_concept_id = @source_concept_id

}};