IF OBJECT_ID('@output_database_schema.@output_table', 'U') IS NOT NULL
	DROP TABLE @output_database_schema.@output_table;

CREATE TABLE @output_database_schema.@output_table (
	cohort_concept_id INT,
	cohort_start_date DATE,
	cohort_end_date DATE,
	subject_id BIGINT
	);