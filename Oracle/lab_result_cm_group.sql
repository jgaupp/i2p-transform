select lab_result_cm_id from lab_result_cm where 'dep' = 'lab_result_cm_initialize.sql'
/
BEGIN
PCORNetLabResultCM(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'lab_result_cm_group', sysdate, count(*), :patient_num_first, :patient_num_last
from lab_result_cm where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'lab_result_cm_group'