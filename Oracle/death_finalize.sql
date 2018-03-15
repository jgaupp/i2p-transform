/** death_finalize - finalize the death table.
*/
select 1 from dual
/
insert into cdm_status (status, last_update, records) select 'death_finalize', sysdate, count(*) from death
/
select 1 from death where rownum = 1