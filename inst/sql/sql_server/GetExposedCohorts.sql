/************************************************************************
@file GetExposedCohorts.sql

Copyright 2023 Observational Health Data Sciences and Informatics

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
SELECT row_id,
	cohort_definition_id AS exposure_id,
	subject_id AS person_id,
	cohort_start_date,
	DATEDIFF(DAY, cohort_start_date, cohort_end_date) AS days_at_risk,
	DATEDIFF(DAY, cohort_start_date, observation_period_end_date) AS days_observed,
	era_number
FROM #cohort_person
