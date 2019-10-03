SELECT standard.concept_id,
	standard.concept_name
INTO #standard_concepts
FROM @cdm_database_schema.concept ancestor
INNER JOIN @cdm_database_schema.concept_ancestor
	ON ancestor_concept_id = ancestor.concept_id
INNER JOIN @cdm_database_schema.concept standard
	ON descendant_concept_id = standard.concept_id
WHERE ancestor.concept_name = '@concept_name';

SELECT source_code.concept_id,
	source_code.concept_name
INTO #mapped_source_codes
FROM @cdm_database_schema.concept source_code
INNER JOIN @cdm_database_schema.concept_relationship
	ON concept_id_1 = source_code.concept_id
INNER JOIN #standard_concepts standard
	ON concept_id_2 = standard.concept_id
WHERE concept_relationship.invalid_reason IS NULL
	AND relationship_id = 'Maps to';

SELECT concept_id,
	code,
	source_table,
	description,
	vocabulary_id
INTO #orphan_codes
FROM (	
	SELECT concept_id,
		concept_code AS code,
		concept_name AS description,
		CAST('concept' AS VARCHAR(21)) AS source_table,
		vocabulary_id
	FROM @cdm_database_schema.concept
	WHERE (
			@concept_name_clause
			)
		AND concept_id NOT IN (
			SELECT concept_id
			FROM #standard_concepts
			)
		AND concept_id NOT IN (
			SELECT concept_id
			FROM #mapped_source_codes
			)

	UNION

	SELECT source_concept_ID AS concept_id,
		source_code AS code,
		source_code_description AS description,
		CAST('source_to_concept_map' AS VARCHAR(21)) AS source_table,
		source_vocabulary_id AS vocabulary_id
	FROM @cdm_database_schema.source_to_concept_map
	WHERE (
			@source_code_description_clause
			)
		AND target_concept_id NOT IN (
			SELECT concept_id
			FROM #standard_concepts
			)
	) temp
