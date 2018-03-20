create index priority_idx on priority (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('PRIORITY');
END;
/
create index location_idx on location (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('LOCATION');
END;
/
create index lab_result_cm_idx on lab_result_cm (PATID, ENCOUNTERID)
/
BEGIN
GATHER_TABLE_STATS('LAB_RESULT_CM');
END;
/
insert into cdm_status (status, last_update, records) select 'lab_result_cm_finalize', sysdate, count(*) from lab_result_cm
/
select 1 from cdm_status where status = 'lab_result_cm_finalize'