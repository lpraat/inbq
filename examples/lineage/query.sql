CREATE TEMP TABLE patient_vitals AS
SELECT 
  p.patient_id,
  p.demographics.age,
  ARRAY(
    SELECT AS STRUCT 
      reading.measurement_type,
      reading.value,
      d.reference_ranges.normal_min,
      d.reference_ranges.normal_max
    FROM UNNEST(p.vital_signs) AS reading
    JOIN `gcp-health-project.reference.diagnostics` d ON reading.measurement_type = d.test_name
  ) AS processed_vitals
FROM `gcp-health-project.patients.records` p;

INSERT INTO gcp-health-project.analytics.patient_summary
WITH health_summary AS (
  SELECT
    patient_id,
    age,
    COUNT(vital.measurement_type) AS total_measurements,
    COUNTIF(vital.value BETWEEN vital.normal_min AND vital.normal_max) AS normal_measurements
  FROM patient_vitals,
  UNNEST(processed_vitals) AS vital
  GROUP BY patient_id, age, processed_vitals
)
SELECT * FROM health_summary;