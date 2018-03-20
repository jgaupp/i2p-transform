select encounterid from encounter where 'dep' = 'encounter_initialize.sql'
/
BEGIN
PCORNetEncounter(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'encounter_group', sysdate, count(distinct patid), :patient_num_first, :patient_num_last
from encounter where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'encounter_group'