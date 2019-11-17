__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!!! THIS SECTION IS NECESSARY FOR AIRFLOW TO RECOGNIZE THE DAG !!!!! 
from airflow import DAG
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
'''
from airflow import DAG
from datetime import datetime, timedelta
from mhs.src.dao.mhs.dao_mongo import DAOMongo
from mhs.dags.mhs_operator import MHSPythonAcquisitionOperator
from mhs.scripts.base_script import DataAcquisitionScript, DataPreparationScript, MongoScript


class AcquisitionFactoryDag(object):

    def __init__(self):
        pass

    def get_params(self):
        raise NotImplemented()

    def get_script(self, **kwargs):
        raise NotImplemented()

    def build_dag(self):
        kwargs = self.get_params()

        config_class = kwargs['config_class']
        config_name = kwargs['config_name']
        documents = DAOMongo.instance()
        params = documents.get_config(config_class=config_class, config_name=config_name)
        mhs_operator = MHSPythonAcquisitionOperator()

        default_args = {
            'owner': 'ComplexCareLab',
            'depends_on_past': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=10),
        }

        dag = DAG(dag_id=f'DAG_ACQUISITION_{config_class}_{config_name}',
                  default_args=default_args,
                  start_date=datetime(2019, 6, 13),
                  schedule_interval=None)

        for (key, value) in params:
            # script_acquisition = UDSMapperDataAcquisitionScript()
            kwargs = {
                'config_class': config_class,
                'config_name': config_name,
                'config_key': key,
                'config_value': value,
            }
            script = self.get_script(**kwargs)
            script_acquisition = DataAcquisitionScript(script)
            operator_acquisition = mhs_operator.create_operator(dag=dag,
                                                                python_callable=script_acquisition,
                                                                op_kwargs=kwargs)

            script_preparation = DataPreparationScript(script)
            operator_preparation = mhs_operator.create_operator(dag=dag,
                                                                python_callable=script_preparation,
                                                                op_kwargs=kwargs)

            script_mongo = MongoScript(script)
            operator_mongo = mhs_operator.create_operator(dag=dag,
                                                          python_callable=script_mongo,
                                                          op_kwargs=kwargs)

            operator_acquisition >> operator_preparation >> operator_mongo

        return dag
        # operator_mongo.execute_callable()


# dag.tasks[4].execute_callable()