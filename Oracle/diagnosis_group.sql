/** diagnosis_group
*/
select diagnosisid from diagnosis where 'dep' = 'diagnosis_initialize.sql'
/
BEGIN
PCORNetDiagnosis(:patient_num_first, :patient_num_last);
END;
/
commit
/
insert into cdm_status (status, last_update, records, group_start, group_end) select 'diagnosis_group', sysdate, count(*), :patient_num_first, :patient_num_last
from diagnosis where patid between cast(:patient_num_first as number) and cast(:patient_num_last as number)
/
select 1 from cdm_status where status = 'diagnosis_group' and group_start = :patient_num_first and group_end = :patient_num_last