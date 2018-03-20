/** death - create and populate the death table.
*/
select patid from demographic where 'dep' = 'demographic.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE death');
END;
/
CREATE TABLE death(
	PATID number(38, 0) NOT NULL,
	DEATH_DATE date NOT NULL,
	DEATH_DATE_IMPUTE varchar(2) NULL,
	DEATH_SOURCE varchar(2) NOT NULL,
	DEATH_MATCH_CONFIDENCE varchar(2) NULL
)
/
create or replace procedure PCORNetDeath(patient_num_first int, patient_num_last int) as
begin

insert /*+ APPEND*/ into death( patid, death_date, death_date_impute, death_source, death_match_confidence)
select distinct pat.patient_num, pat.death_date,
case when vital_status_cd like 'X%' then 'B'
  when vital_status_cd like 'M%' then 'D'
  when vital_status_cd like 'Y%' then 'N'
  else 'OT'
  end death_date_impute,
  'NI' death_source,
  'NI' death_match_confidence
from (
	/* KUMC specific fix to address unknown death dates */
  select
    ibp.patient_num,
    case when ibf.concept_cd is not null then DATE '2100-12-31' -- in accordance with the CDM v3 spec
      else ibp.death_date end death_date,
    case when ibf.concept_cd is not null then 'OT'
      else upper(ibp.vital_status_cd) end vital_status_cd
  from i2b2patient ibp
  left join i2b2fact ibf
    on ibp.patient_num=ibf.patient_num
    and ibf.CONCEPT_CD='DEM|VITAL:yu'
) pat
where (pat.death_date is not null or vital_status_cd like 'Z%') and pat.patient_num in (select patid from demographic)
and pat.patient_num between patient_num_first and patient_num_last;

end;
/
insert into cdm_status (status, last_update) select 'death_initialize', sysdate from dual
/
select 1 from cdm_status where status = 'death_initialize'