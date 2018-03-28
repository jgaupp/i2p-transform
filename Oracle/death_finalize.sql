/** death_finalize - finalize the death table.
*/

select 1 from dual
/
insert into cdm_status (status, last_update, records) select 'death_finalize', sysdate, count(*) from death
/
select case when records > 0 then 1 else 0 end from cdm_status where status = 'death_finalize'