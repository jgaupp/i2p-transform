/** death_group - populate the death table.
*/
select patid from death where 'dep' = 'death_initialize.sql'
/
BEGIN
pcornetdeath(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'death_group', sysdate, count(distinct patid), :patient_num_first, :patient_num_last
from death where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'death_group' and group_start = :patient_num_first and group_end = :patient_num_last