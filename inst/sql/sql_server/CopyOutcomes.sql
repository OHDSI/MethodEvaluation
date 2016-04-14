{DEFAULT @cdm_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_database_schema = 'CDM4_SIM' } 
{DEFAULT @outcome_table = 'condition_era' }
{DEFAULT @source_concept_id = 0}
{DEFAULT @target_concept_id = 0}
{DEFAULT @cohort_definition_id = 'cohort_concept_id'}

{@outcome_table == 'condition_era' } ? {
	SELECT @target_concept_id AS @cohort_definition_id,
		condition_era_start_date AS cohort_start_date,
		condition_era_end_date AS cohort_end_date,
		person_id AS subject_id  
	FROM @cdm_database_schema.condition_era
	WHERE condition_concept_id = @source_concept_id
} : {
	SELECT @target_concept_id AS @cohort_definition_id,
		cohort_start_date,
		cohort_end_date,
		subject_id
	FROM @outcome_database_schema.@outcome_table
	WHERE @cohort_definition_id = @source_concept_id
}
