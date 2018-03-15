/** diagnosis_finalize - finalize the diagnosis table.
*/
create index sourcefact_idx on sourcefact (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('SOURCEFACT');
END;
/
create index pdxfact_idx on pdxfact (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('PDXFACT');
END;
/
create index originfact_idx on originfact (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('ORIGINFACT');
END;
/
create index diagnosis_idx on diagnosis (PATID, ENCOUNTERID)
/
BEGIN
GATHER_TABLE_STATS('DIAGNOSIS');
END;
/
insert into cdm_status (status, last_update, records) select 'diagnosis_finalize', sysdate, count(*) from diagnosis
/
select 1 from cdm_status where status = 'diagnosis_finalize'