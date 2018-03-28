create index dispensing_idx on dispensing (PATID)
/
BEGIN
GATHER_TABLE_STATS('DISPENSING');
END;
/
insert into cdm_status (status, last_update, records) select 'dispensing_finalize', sysdate, count(*) from dispensing
/
select case when records > 0 then 1 else 0 end from cdm_status where status = 'dispensing_finalize'