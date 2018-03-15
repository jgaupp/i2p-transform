/** condition_test.
*/
begin
pcornetcondition_test(:patient_num_first, :patient_num_last);
end;
/
commit
/
select 1 from condition where rownum = 1