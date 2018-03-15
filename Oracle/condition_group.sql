/** condition_group - populate the condition table.
*/
select conditionid from condition where 'dep' = 'condition_initialize.sql'
/
BEGIN
pcornetcondition(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'condition_group', sysdate, count(*), :patient_num_first, :patient_num_last
from condition where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'condition_group'