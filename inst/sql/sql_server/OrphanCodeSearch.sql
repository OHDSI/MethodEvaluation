WITH drug_concept_count
AS (
	SELECT code,
		COUNT(*) AS drug_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.drug_exposure
		ON drug_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
drug_code_count
AS (
	SELECT code,
		COUNT(*) AS drug_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.drug_exposure
		ON drug_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	),
condition_concept_count
AS (
	SELECT code,
		COUNT(*) AS condition_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON condition_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
condition_code_count
AS (
	SELECT code,
		COUNT(*) AS condition_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON condition_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	),
procedure_concept_count
AS (
	SELECT code,
		COUNT(*) AS procedure_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.procedure_occurrence
		ON procedure_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
procedure_code_count
AS (
	SELECT code,
		COUNT(*) AS procedure_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.procedure_occurrence
		ON procedure_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	),
device_concept_count
AS (
	SELECT code,
		COUNT(*) AS device_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.device_exposure
		ON device_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
device_code_count
AS (
	SELECT code,
		COUNT(*) AS device_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.device_exposure
		ON device_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	),
measurement_concept_count
AS (
	SELECT code,
		COUNT(*) AS measurement_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.measurement
		ON measurement_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
measurement_code_count
AS (
	SELECT code,
		COUNT(*) AS measurement_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.measurement
		ON measurement_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	),
observation_concept_count
AS (
	SELECT code,
		COUNT(*) AS observation_concept_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.observation
		ON observation_source_concept_id = concept_id
	WHERE concept_id != 0
	GROUP BY code
	),
observation_code_count
AS (
	SELECT code,
		COUNT(*) AS observation_code_count
	FROM #orphan_codes
	INNER JOIN @cdm_database_schema.observation
		ON observation_source_value = code
	WHERE concept_id = 0
	GROUP BY code
	)
SELECT orphan_codes.concept_id,
	orphan_codes.code,
	description,
	source_table,
	vocabulary_id,
	CASE 
		WHEN drug_concept_count IS NULL
			THEN 0
		ELSE drug_concept_count
		END + CASE 
		WHEN drug_code_count IS NULL
			THEN 0
		ELSE drug_code_count
		END AS drug_count,
	CASE 
		WHEN condition_concept_count IS NULL
			THEN 0
		ELSE condition_concept_count
		END + CASE 
		WHEN condition_code_count IS NULL
			THEN 0
		ELSE condition_code_count
		END AS condition_count,
	CASE 
		WHEN procedure_concept_count IS NULL
			THEN 0
		ELSE procedure_concept_count
		END + CASE 
		WHEN procedure_code_count IS NULL
			THEN 0
		ELSE procedure_code_count
		END AS procedure_count,
	CASE 
		WHEN device_concept_count IS NULL
			THEN 0
		ELSE device_concept_count
		END + CASE 
		WHEN device_code_count IS NULL
			THEN 0
		ELSE device_code_count
		END AS device_count,
	CASE 
		WHEN measurement_concept_count IS NULL
			THEN 0
		ELSE measurement_concept_count
		END + CASE 
		WHEN measurement_code_count IS NULL
			THEN 0
		ELSE measurement_code_count
		END AS measurement_count,
	CASE 
		WHEN observation_concept_count IS NULL
			THEN 0
		ELSE observation_concept_count
		END + CASE 
		WHEN observation_code_count IS NULL
			THEN 0
		ELSE observation_code_count
		END AS observation_count
FROM #orphan_codes orphan_codes
LEFT JOIN drug_concept_count
	ON orphan_codes.code = drug_concept_count.code
LEFT JOIN drug_code_count
	ON orphan_codes.code = drug_code_count.code
LEFT JOIN condition_concept_count
	ON orphan_codes.code = condition_concept_count.code
LEFT JOIN condition_code_count
	ON orphan_codes.code = condition_code_count.code
LEFT JOIN procedure_concept_count
	ON orphan_codes.code = procedure_concept_count.code
LEFT JOIN procedure_code_count
	ON orphan_codes.code = procedure_code_count.code
LEFT JOIN device_concept_count
	ON orphan_codes.code = device_concept_count.code
LEFT JOIN device_code_count
	ON orphan_codes.code = device_code_count.code
LEFT JOIN measurement_concept_count
	ON orphan_codes.code = measurement_concept_count.code
LEFT JOIN measurement_code_count
	ON orphan_codes.code = measurement_code_count.code
LEFT JOIN observation_concept_count
	ON orphan_codes.code = observation_concept_count.code
LEFT JOIN observation_code_count
	ON orphan_codes.code = observation_code_count.code;
