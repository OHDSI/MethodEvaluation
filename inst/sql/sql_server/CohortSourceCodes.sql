SELECT codeset_id,
	concept.concept_id,
	concept.concept_name,
	source_value,
	source_concept.vocabulary_id AS source_vocabulary_id,
	source_concept_id,
	source_concept.concept_name AS source_concept_name,
	event_year,
	event_month,
	person_count
FROM (
	SELECT codeset_id,
		concept_id,
		condition_source_value AS source_value,
		condition_source_concept_id AS source_concept_id,
		YEAR(condition_start_date) AS event_year,
		MONTH(condition_start_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.condition_occurrence
		ON condition_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON condition_occurrence.person_id = observation_period.person_id
			AND condition_start_date >= observation_period_start_date
			AND condition_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		condition_source_value,
		condition_source_concept_id,
		YEAR(condition_start_date),
		MONTH(condition_start_date)
	
	UNION ALL
	
	SELECT codeset_id,
		concept_id,
		drug_source_value AS source_value,
		drug_source_concept_id AS source_concept_id,
		YEAR(drug_exposure_start_date) AS event_year,
		MONTH(drug_exposure_start_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.drug_exposure
		ON drug_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON drug_exposure.person_id = observation_period.person_id
			AND drug_exposure_start_date >= observation_period_start_date
			AND drug_exposure_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		drug_source_value,
		drug_source_concept_id,
		YEAR(drug_exposure_start_date),
		MONTH(drug_exposure_start_date)
	
	UNION ALL
	
	SELECT codeset_id,
		concept_id,
		procedure_source_value AS source_value,
		procedure_source_concept_id AS source_concept_id,
		YEAR(procedure_date) AS event_year,
		MONTH(procedure_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.procedure_occurrence
		ON procedure_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON procedure_occurrence.person_id = observation_period.person_id
			AND procedure_date >= observation_period_start_date
			AND procedure_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		procedure_source_value,
		procedure_source_concept_id,
		YEAR(procedure_date),
		MONTH(procedure_date)
	
	UNION ALL
	
	SELECT codeset_id,
		concept_id,
		measurement_source_value AS source_value,
		measurement_source_concept_id AS source_concept_id,
		YEAR(measurement_date) AS event_year,
		MONTH(measurement_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.measurement
		ON measurement_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON measurement.person_id = observation_period.person_id
			AND measurement_date >= observation_period_start_date
			AND measurement_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		measurement_source_value,
		measurement_source_concept_id,
		YEAR(measurement_date),
		MONTH(measurement_date)
	
	UNION ALL
	
	SELECT codeset_id,
		concept_id,
		observation_source_value AS source_value,
		observation_source_concept_id AS source_concept_id,
		YEAR(observation_date) AS event_year,
		MONTH(observation_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.observation
		ON observation_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON observation.person_id = observation_period.person_id
			AND observation_date >= observation_period_start_date
			AND observation_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		observation_source_value,
		observation_source_concept_id,
		YEAR(observation_date),
		MONTH(observation_date)
		
	UNION ALL
	
	SELECT codeset_id,
		concept_id,
		visit_source_value AS source_value,
		visit_source_concept_id AS source_concept_id,
		YEAR(visit_start_date) AS event_year,
		MONTH(visit_start_date) AS event_month,
		COUNT(DISTINCT observation_period.person_id) AS person_count
	FROM #Codesets
	INNER JOIN @cdm_database_schema.visit_occurrence
		ON visit_concept_id = concept_id
	INNER JOIN @cdm_database_schema.observation_period
		ON visit_occurrence.person_id = observation_period.person_id
			AND visit_start_date >= observation_period_start_date
			AND visit_start_date <= observation_period_end_date
	GROUP BY codeset_id,
		concept_id,
		visit_source_value,
		visit_source_concept_id,
		YEAR(visit_start_date),
		MONTH(visit_start_date)

	) person_counts
INNER JOIN @cdm_database_schema.concept
	ON person_counts.concept_id = concept.concept_id
LEFT JOIN @cdm_database_schema.concept source_concept
	ON source_concept_id = source_concept.concept_id;
