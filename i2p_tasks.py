"""i2p_tasks -- Luigi CDM task support.
"""

from etl_tasks import SqlScriptTask
from param_val import IntParam
from script_lib import Script
from sql_syntax import Environment
from sqlalchemy.engine import RowProxy
from sqlalchemy.exc import DatabaseError
from typing import cast, List, Type

import luigi

class CDMScriptTask(SqlScriptTask):

    @property
    def variables(self) -> Environment:
        return dict(datamart_id='C4UK', datamart_name='University of Kansas', i2b2_data_schema='BLUEHERONDATA',
                    min_pat_list_date_dd_mon_rrrr='01-Jan-2010', min_visit_date_dd_mon_rrrr='01-Jan-2010',
                    i2b2_meta_schema='BLUEHERONMETADATA', enrollment_months_back='2', network_id='C4',
                    network_name='GPC')


class CDMPatientGroupTask(CDMScriptTask):
    patient_num_first = IntParam()
    patient_num_last = IntParam()
    patient_num_qty = IntParam(significant=False, default=-1)
    group_num = IntParam(significant=False, default=-1)
    group_qty = IntParam(significant=False, default=-1)

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(
            patient_num_first=self.patient_num_first, patient_num_last=self.patient_num_last))


class _PatientNumGrouped(luigi.WrapperTask):
    group_tasks = cast(List[Type[CDMPatientGroupTask]], [])  # abstract

    def requires(self) -> List[luigi.Task]:
        deps = []  # type: List[luigi.Task]
        for group_task in self.group_tasks:
            survey = patient_chunks_survey()
            deps += [survey]
            results = survey.results()
            if results:
                deps += [
                    group_task(
                        group_num=ntile.chunk_num,
                        group_qty=len(results),
                        patient_num_qty=ntile.patient_num_qty,
                        patient_num_first=ntile.patient_num_first,
                        patient_num_last=ntile.patient_num_last)
                    for ntile in results
                ]
        return deps


class patient_chunks_survey(SqlScriptTask):
    script = Script.patient_chunks_survey
    patient_chunks = IntParam(default=10)
    patient_chunk_max = IntParam(default=None)

    @property
    def variables(self) -> Environment:
        return dict(chunk_qty=str(self.patient_chunks))

    def run(self) -> None:
        SqlScriptTask.run_bound(self, script_params=dict(chunk_qty=str(self.patient_chunks)))

    def results(self) -> List[RowProxy]:
        with self.connection(event='survey results') as lc:
            q = '''
               select chunk_num
                 , patient_num_qty
                 , patient_num_first
                 , patient_num_last
               from patient_chunks
               where chunk_qty = :chunk_qty
                 and (:chunk_max is null or
                      chunk_num <= :chunk_max)
               order by chunk_num
             '''
            params = dict(chunk_max=self.patient_chunk_max, chunk_qty=self.patient_chunks)

            try:
                return lc.execute(q, params=params).fetchall()
            except DatabaseError:
                return []


class condition_finalize(CDMScriptTask):
    script = Script.condition_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [condition_wrapper()]


class condition_group(CDMPatientGroupTask):
    script = Script.condition_group
    resources = {'condition_group_resource': 1}


class condition_initialize(CDMScriptTask):
    script = Script.condition_initialize


class condition_wrapper(_PatientNumGrouped):
    group_tasks = [condition_group]


class death_finalize(CDMScriptTask):
    script = Script.death_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [death_wrapper()]


class death_group(CDMPatientGroupTask):
    script = Script.death_group
    resources = {'death_group_resource': 1}


class death_initialize(CDMScriptTask):
    script = Script.death_initialize


class death_wrapper(_PatientNumGrouped):
    group_tasks = [death_group]


class death_cause(CDMScriptTask):
    script = Script.death_cause

'''
class demographic_finalize(CDMScriptTask):
    script = Script.demographic_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [demographic_wrapper()]


class demographic_group(CDMPatientGroupTask):
    script = Script.demographic_group


class demographic_intialize(CDMScriptTask):
    script = Script.demographic_initialize


class demographic_wrapper(_PatientNumGrouped):
    group_tasks = [demographic_group]
'''

class diagnosis_finalize(CDMScriptTask):
    script = Script.diagnosis_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [diagnosis_wrapper()]


class diagnosis_group(CDMPatientGroupTask):
    script = Script.diagnosis_group
    resources = {'diagnosis_group_resource': 1}


class diagnosis_initialize(CDMScriptTask):
    script = Script.diagnosis_initialize


class diagnosis_wrapper(_PatientNumGrouped):
    group_tasks = [diagnosis_group]


class dispensing_finalize(CDMScriptTask):
    script = Script.dispensing_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [dispensing_wrapper()]


class dispensing_group(CDMPatientGroupTask):
    script = Script.dispensing_group
    resources = {'dispensing_group_resource': 1}

class dispensing_initialize(CDMScriptTask):
    script = Script.dispensing_initialize


class dispensing_wrapper(_PatientNumGrouped):
    group_tasks = [dispensing_group]


'''
class encounter_finalize(CDMScriptTask):
    script = Script.encounter_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [encounter_wrapper()]


class encounter_group(CDMPatientGroupTask):
    script = Script.encounter_group


class encounter_initialize(CDMScriptTask):
    script = Script.encounter_initialize


class encounter_wrapper(_PatientNumGrouped):
    group_tasks = [encounter_group]


class enrollment_finalize(CDMScriptTask):
    script = Script.enrollment_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [enrollment_wrapper()]


class enrollment_group(CDMPatientGroupTask):
    script = Script.enrollment_group


class enrollment_initialize(CDMScriptTask):
    script = Script.enrollment_initialize


class enrollment_wrapper(_PatientNumGrouped):
    group_tasks = [enrollment_group]
'''


class harvest(CDMScriptTask):
    script = Script.harvest

    def requires(self):
        return CDMScriptTask.requires(self) + [condition_finalize(), death_finalize(), death_cause(), diagnosis_finalize(),
                                               dispensing_finalize(), enrollment(), lab_result_cm(), med_admin(), obs_clin(),
                                               obs_gen(), pcornet_trial(), prescribing(), pro_cm(), procedures(),
                                               provider(), vital()]


class lab_result_cm_finalize(CDMScriptTask):
    script = Script.lab_result_cm_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [lab_result_cm_wrapper()]


class lab_result_cm_group(CDMPatientGroupTask):
    script = Script.lab_result_cm_group


class lab_result_cm_initialize(CDMScriptTask):
    script = Script.lab_result_cm_initialize


class lab_result_cm_wrapper(_PatientNumGrouped):
    group_tasks = [lab_result_cm_group]


'''
class med_admin_finalize(CDMScriptTask):
    script = Script.med_admin_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [med_admin_wrapper()]


class med_admin_group(CDMPatientGroupTask):
    script = Script.med_admin_group


class med_admin_initialize(CDMScriptTask):
    script = Script.med_admin_initialize


class med_admin_wrapper(_PatientNumGrouped):
    group_tasks = [med_admin_group]
'''

class obs_clin(CDMScriptTask):
    script = Script.obs_clin


class obs_gen(CDMScriptTask):
    script = Script.obs_gen


class pcornet_init(CDMScriptTask):
    script = Script.pcornet_init


class pcornet_loader(CDMScriptTask):
    script = Script.pcornet_loader

    def requires(self):
        return CDMScriptTask.requires(self) + [harvest()]


class pcornet_trial(CDMScriptTask):
    script = Script.pcornet_trial

'''
class prescribing_finalize(CDMScriptTask):
    script = Script.prescribing_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [prescribing_wrapper()]


class prescribing_group(CDMPatientGroupTask):
    script = Script.prescribing_group


class prescribing_initialize(CDMScriptTask):
    script = Script.prescribing_initialize


class prescribing_wrapper(_PatientNumGrouped):
    group_tasks = [prescribing_group]
'''


class pro_cm(CDMScriptTask):
    script = Script.pro_cm


'''
class procedures_finalize(CDMScriptTask):
    script = Script.procedures_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [procedures_wrapper()]


class procedures_group(CDMPatientGroupTask):
    script = Script.procedures_group


class procedures_initialize(CDMScriptTask):
    script = Script.procedures_initialize


class procedures_wrapper(_PatientNumGrouped):
    group_tasks = [procedures_group]


class provider_finalize(CDMScriptTask):
    script = Script.provider_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [provider_wrapper()]


class provider_group(CDMPatientGroupTask):
    script = Script.provider_group


class provider_initialize(CDMScriptTask):
    script = Script.provider_initialize


class provider_wrapper(_PatientNumGrouped):
    group_tasks = [provider_group]


class vital_finalize(CDMScriptTask):
    script = Script.vital_finalize

    def requires(self):
        return CDMScriptTask.requires(self) + [vital_wrapper()]


class vital_group(CDMPatientGroupTask):
    script = Script.vital_group


class vital_initialize(CDMScriptTask):
    script = Script.vital_initialize


class vital_wrapper(_PatientNumGrouped):
    group_tasks = [vital_group]
'''


##TODO update classes below to group processing


class demographic(CDMScriptTask):
    script = Script.demographic


#class dispensing(CDMScriptTask):
#    script = Script.dispensing


class encounter(CDMScriptTask):
    script = Script.encounter


class enrollment(CDMScriptTask):
    script = Script.enrollment


class lab_result_cm(CDMScriptTask):
    script = Script.lab_result_cm


class med_admin(CDMScriptTask):
    script = Script.med_admin


class prescribing(CDMScriptTask):
    script = Script.prescribing


class procedures(CDMScriptTask):
    script = Script.procedures


class provider(CDMScriptTask):
    script = Script.provider


class vital(CDMScriptTask):
    script = Script.vital
