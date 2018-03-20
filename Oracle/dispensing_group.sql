select dispensingid from dispensing where 'dep' = 'dispensing_initialize.sql'
/
BEGIN
PCORNetDispensing(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'dispensing_group', sysdate, count(*), :patient_num_first, :patient_num_last
from dispensing where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'dispensing_group'