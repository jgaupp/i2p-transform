create index dispensing_idx on dispensing (PATID)
/
BEGIN
GATHER_TABLE_STATS('DISPENSING');
END;
/
insert into cdm_status (status, last_update, records) select 'dispensing_finalize', sysdate, count(*) from dispensing
/
select 1 from cdm_status where status = 'dispensing_finalize'