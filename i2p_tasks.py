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


class condition(CDMScriptTask):
    script = Script.condition


class condition_finalize(CDMScriptTask):
    script = Script.condition_finalize

    def requires(self):
        deps1 = CDMScriptTask.requires(self)
        return deps1 + [condition_group()]


class condition_group_task(CDMPatientGroupTask):
    script = Script.condition_group


class condition_initialize(CDMScriptTask):
    script = Script.condition_initialize


class condition_group(_PatientNumGrouped):
    group_tasks = [condition_group_task]


class death(CDMScriptTask):
    script = Script.death


class death_cause(CDMScriptTask):
    script = Script.death_cause


class demographic(CDMScriptTask):
    script = Script.demographic


class diagnosis(CDMScriptTask):
    script = Script.diagnosis


class dispensing(CDMScriptTask):
    script = Script.dispensing


class encounter(CDMScriptTask):
    script = Script.encounter


class enrollment(CDMScriptTask):
    script = Script.enrollment


class harvest(CDMScriptTask):
    script = Script.harvest


class lab_result_cm(CDMScriptTask):
    script = Script.lab_result_cm


class med_admin(CDMScriptTask):
    script = Script.med_admin


class med_admin_init(CDMScriptTask):
    script = Script.med_admin_init


class obs_clin(CDMScriptTask):
    script = Script.obs_clin


class obs_gen(CDMScriptTask):
    script = Script.obs_gen


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


class pcornet_init(CDMScriptTask):
    script = Script.pcornet_init


class pcornet_loader(CDMScriptTask):
    script = Script.pcornet_loader


class pcornet_trial(CDMScriptTask):
    script = Script.pcornet_trial


class prescribing(CDMScriptTask):
    script = Script.prescribing


class pro_cm(CDMScriptTask):
    script = Script.pro_cm


class procedures(CDMScriptTask):
    script = Script.procedures


class provider(CDMScriptTask):
    script = Script.provider


class vital(CDMScriptTask):
    script = Script.vital
