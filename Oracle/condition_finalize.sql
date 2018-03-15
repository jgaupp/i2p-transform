/** condition_finalize - finalize the condition table.
*/

create index sourcefact2_idx on sourcefact2 (patient_num, encounter_num, provider_id, concept_cd, start_date)
/
BEGIN
GATHER_TABLE_STATS('SOURCEFACT2');
END;
/
create index condition_idx on condition (PATID, ENCOUNTERID)
/
BEGIN
GATHER_TABLE_STATS('CONDITION');
END;
/
insert into cdm_status (status, last_update, records) select 'condition_finalize', sysdate, count(*) from condition
/
select 1 from cdm_status where status = 'condition_finalize'