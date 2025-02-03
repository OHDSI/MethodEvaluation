/************************************************************************
@file GetExposedCohorts.sql

Copyright 2025 Observational Health Data Sciences and Informatics

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
{DEFAULT @sample_size = 100000}
{DEFAULT @cohort_ids = 1,2,3}

--HINT DISTRIBUTE_ON_KEY(subject_id)
SELECT row_id,
	cohort_definition_id,
	subject_id,
	cohort_start_date,
	cohort_end_date,
	era_number
INTO #sampled_person
FROM (
	SELECT ROW_NUMBER() OVER (ORDER BY NEWID()) AS rn,
		row_id,
		cohort_definition_id,
		subject_id,
		cohort_start_date,
		cohort_end_date,
		era_number
	FROM #cohort_person
	WHERE cohort_definition_id IN (@cohort_ids)
) temp
WHERE rn <= @sample_size;
