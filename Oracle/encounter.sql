/** encounter - create and populate the encounter table.
*/

select patid from demographic where 'dep' = 'demographic.sql'
/
BEGIN
PMN_DROPSQL('DROP TABLE encounter');
END;
/
CREATE TABLE encounter(
	PATID varchar(50) NOT NULL,
	ENCOUNTERID varchar(50) NOT NULL,
	ADMIT_DATE date NULL,
	ADMIT_TIME varchar(5) NULL,
	DISCHARGE_DATE date NULL,
	DISCHARGE_TIME varchar(5) NULL,
	PROVIDERID varchar(50) NULL,
	FACILITY_LOCATION varchar(3) NULL,
	ENC_TYPE varchar(2) NOT NULL,
	FACILITYID varchar(50) NULL,
	DISCHARGE_DISPOSITION varchar(2) NULL,
	DISCHARGE_STATUS varchar(2) NULL,
	DRG varchar(3) NULL,
	DRG_TYPE varchar(2) NULL,
	ADMITTING_SOURCE varchar(2) NULL,
	RAW_SITEID varchar (50) NULL,
	RAW_ENC_TYPE varchar(50) NULL,
	RAW_DISCHARGE_DISPOSITION varchar(50) NULL,
	RAW_DISCHARGE_STATUS varchar(50) NULL,
	RAW_DRG_TYPE varchar(50) NULL,
	RAW_ADMITTING_SOURCE varchar(50) NULL
)
/
BEGIN
PMN_DROPSQL('DROP TABLE drg');
END;
/
CREATE TABLE drg (
  PATIENT_NUM NUMBER(38) NOT NULL,
  ENCOUNTER_NUM NUMBER(38) NOT NULL,
  DRG_TYPE VARCHAR2(2),
  DRG VARCHAR2(3),
  RN NUMBER
)
/
create or replace procedure PCORNetEncounter as
begin

PMN_DROPSQL('drop index encounter_pk');
PMN_DROPSQL('drop index encounter_idx');
PMN_DROPSQL('drop index drg_idx');

execute immediate 'truncate table encounter';
execute immediate 'truncate table drg';

insert into drg
select * from
(select patient_num,encounter_num,drg_type, drg,row_number() over (partition by  patient_num, encounter_num order by drg_type desc) AS rn from
(select patient_num,encounter_num,drg_type,max(drg) drg  from
(select distinct f.patient_num,encounter_num,SUBSTR(c_fullname,22,2) drg_type,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,3) drg from i2b2fact f
inner join demographic d on f.patient_num=d.patid
inner join pcornet_enc enc on enc.c_basecode  = f.concept_cd
and enc.c_fullname like '\PCORI\ENCOUNTER\DRG\%') drg1 group by patient_num,encounter_num,drg_type) drg) drg
where rn=1;

execute immediate 'create index drg_idx on drg (patient_num, encounter_num)';
--GATHER_TABLE_STATS('drg');

insert into encounter(PATID,ENCOUNTERID,admit_date ,ADMIT_TIME ,
		DISCHARGE_DATE ,DISCHARGE_TIME ,PROVIDERID ,FACILITY_LOCATION
		,ENC_TYPE ,FACILITYID ,DISCHARGE_DISPOSITION ,
		DISCHARGE_STATUS ,DRG ,DRG_TYPE ,ADMITTING_SOURCE)
select distinct v.patient_num, v.encounter_num,
	start_Date,
	to_char(start_Date,'HH24:MI'),
	end_Date,
	to_char(end_Date,'HH24:MI'),
	providerid,
  'NI' location_zip, /* See TODO above */
(case when pcori_enctype is not null then pcori_enctype else 'UN' end) enc_type,
  'NI' facility_id,  /* See TODO above */
  CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE  discharge_disposition END,
  CASE WHEN pcori_enctype='AV' THEN 'NI' ELSE discharge_status END,
  drg.drg, drg_type,
  CASE WHEN admitting_source IS NULL THEN 'NI' ELSE admitting_source END admitting_source
from i2b2visit v inner join demographic d on v.patient_num=d.patid
left outer join drg -- This section is bugfixed to only include 1 drg if multiple DRG types exist in a single encounter...
  on drg.patient_num=v.patient_num and drg.encounter_num=v.encounter_num
left outer join
-- Encounter type. Note that this requires a full table scan on the ontology table, so it is not particularly efficient.
(select patient_num, encounter_num, inout_cd,SUBSTR(pcori_basecode,INSTR(pcori_basecode, ':')+1,2) pcori_enctype from i2b2visit v
 inner join pcornet_enc e on c_dimcode like '%'''||inout_cd||'''%' and e.c_fullname like '\PCORI\ENCOUNTER\ENC_TYPE\%') enctype
  on enctype.patient_num=v.patient_num and enctype.encounter_num=v.encounter_num;

execute immediate 'create unique index encounter_pk on encounter (ENCOUNTERID)';
execute immediate 'create index encounter_idx on encounter (PATID, ENCOUNTERID)';
GATHER_TABLE_STATS('ENCOUNTER');

end PCORNetEncounter;
/
BEGIN
PCORNetEncounter();
END;
/
insert into cdm_status (status, last_update, records) select 'encounter', sysdate, count(*) from encounter
/
select 1 from cdm_status where status = 'encounter'
--SELECT count(ENCOUNTERID) from encounter where rownum = 1