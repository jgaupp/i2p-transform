create index drg_idx on drg (patient_num, encounter_num)
/
BEGIN
GATHER_TABLE_STATS('drg');
END;
/
create unique index encounter_pk on encounter (ENCOUNTERID)
/
create index encounter_idx on encounter (PATID, ENCOUNTERID)
/
BEGIN
GATHER_TABLE_STATS('ENCOUNTER');
END;
/
insert into cdm_status (status, last_update, records) select 'encounter_finalize', sysdate, count(*) from encounter
/
select 1 from cdm_status where status = 'encounter_finalize'