__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!!! THIS SECTION IS NECESSARY FOR AIRFLOW TO RECOGNIZE THE DAG !!!!! 
from airflow import DAG
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
'''
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from mhs.scripts.shared_care_areas.shared_care_area_script import SharedCareAreaScript
# from mhs.src.dao.discharges_dao import DischargesDAO
from mhs.src.library.network.community_detection import CommunityDetection
from mhs.src.shared_care_areas.networks.network_shared_care import NetworkSharedCare
from mhs.src.dao.mhs.documents_mhs import HospitalDischargeDocument
from mhs.src.library.file.utils import get_filename
from mhs.src.shared_care_areas.metrics.metrics import MetricsSharedCareAreaFactory
from mhs.src.library.network.network_metric import NetworkMetricFactory


class SharedCareAreaFactoryDag(object):

    @staticmethod
    def get_airflow_id(airflow_id: str, **kwargs):

        id = '_'.join([str(i) for i in kwargs.values()])
        return get_filename(f'{airflow_id}_{id}')

    @staticmethod
    def get_id(**kwargs):
        pass

    def get_operator_build_network(self, dag, method, discharge_type, year):
        kwargs = {
            'method': method,
            'discharge_type': discharge_type,
            'year': year,

        }
        script_sca = SharedCareAreaScript()
        task_id = self.get_airflow_id(f'BUILD_NETWORK', **kwargs)
        operator = PythonOperator(dag=dag,
                                  task_id=task_id,
                                  python_callable=script_sca.operator_create_network,
                                  op_kwargs=kwargs,
                                  pool='POOL_SCA_FILE',
                                  priority_weight=1)

        return operator

    def get_operator_calculate_network_metrics(self, dag, method, discharge_type, year, metric):
        kwargs = {
            'method': method,
            'discharge_type': discharge_type,
            'year': year,
            'metric': metric

        }
        script_sca = SharedCareAreaScript()
        task_id = self.get_airflow_id(f'CALCULATE_NETWORK_METRICS', **kwargs)
        operator = PythonOperator(dag=dag,
                                  task_id=task_id,
                                  python_callable=script_sca.operator_calculate_network_metrics,
                                  op_kwargs=kwargs,
                                  pool='POOL_SCA_MEMORY',
                                  priority_weight=1)

        return operator

    def get_operator_extract_communities(self, dag, method, discharge_type, year, type_community_detection):
        script_sca = SharedCareAreaScript()
        kwargs = {

            'method': method,
            'type_community_detection': type_community_detection,
            'year': year,
            'discharge_type': discharge_type,
        }
        task_id = self.get_airflow_id(f'FIND_COMMUNITIES', **kwargs)
        operator = PythonOperator(dag=dag,
                                  task_id=task_id,
                                  python_callable=script_sca.operator_extract_communities,
                                  op_kwargs=kwargs,
                                  pool='POOL_SCA_MEMORY',
                                  priority_weight=1)
        return operator

    def get_operator_plot_map(self, dag, method, discharge_type, year, type_community_detection):
        script_sca = SharedCareAreaScript()
        kwargs = {
            'method': method,
            'type_community_detection': type_community_detection,
            'year': year,
            'discharge_type': discharge_type,
        }

        task_id = self.get_airflow_id(f'PLOT_MAPS', **kwargs)
        operator = PythonOperator(dag=dag,
                                  task_id=task_id,
                                  python_callable=script_sca.operator_plot_map,
                                  op_kwargs=kwargs,
                                  pool='POOL_SCA_FILE',
                                  priority_weight=1)
        return operator

    def get_operator_sca_metrics(self, dag, method, discharge_type, year, type_community_detection, metric):
        kwargs = {
            'method': method,
            'year': year,
            'discharge_type': discharge_type,
            'type_community_detection': type_community_detection,
            'metric': metric
        }
        script_sca = SharedCareAreaScript()
        task_id = self.get_airflow_id(f'CALCULATE_COMMUNITY_METRICS', **kwargs)
        operator = PythonOperator(dag=dag,
                                  task_id=task_id,
                                  python_callable=script_sca.operator_calculate_metrics,
                                  op_kwargs=kwargs,
                                  pool='POOL_SCA_MONGO',
                                  priority_weight=1)
        return operator

    # def __create_dag__(self, method, type_community_detection):
    def __create_dag__(self, method):
        default_args = {
            'owner': 'ComplexCareLab',
            'depends_on_past': True,
            'retries': 4,
            'retry_delay': timedelta(minutes=2),
            # 'execution_timeout': timedelta(hours=3),

            # 'wait_for_downstream': True
        }

        # dag_id = self.get_airflow_id(f'DAG_SHARED_CARE_AREA', method=method,
        #                              type_community_detection=type_community_detection)
        dag_id = self.get_airflow_id(f'DAG_SHARED_CARE_AREA', method=method)

        dag = DAG(dag_id=dag_id,
                  default_args=default_args,
                  start_date=datetime(2019, 6, 13),
                  # schedule_interval='@once'
                  schedule_interval=None
                  )

        # var_year = Variable.get('SHARED_CARE_AREA_YEARS', default_var=None)

        # dao_discharges = DischargesDAO()
        # list_years = [2017]
        list_years = list([2012, 2013, 2014, 2015, 2016, 2017, 2018])
        # list_years.append(None)
        # list_discharge_types = [None]
        list_discharge_types = HospitalDischargeDocument.TYPES_DISCHARGE
        # list_discharge_types.append(None)

        # last_dag = None
        for discharge_type in list_discharge_types:

            # discharge_type_id = str(discharge_type).replace(" ", "")
            for year in list_years:
                op_network_build = self.get_operator_build_network(
                    dag=dag,
                    method=method,
                    discharge_type=discharge_type,
                    year=year)

                list_metrics = NetworkMetricFactory.get_types()
                for metric in list_metrics:
                    op_calculate_network_metrics = self.get_operator_calculate_network_metrics(
                        dag=dag,
                        method=method,
                        discharge_type=discharge_type,
                        year=year,
                        metric=metric)

                    op_calculate_network_metrics.set_upstream(op_network_build)

                list_type_community_detection = CommunityDetection.TYPES

                for type_community_detection in list_type_community_detection:

                    op_community_detection = self.get_operator_extract_communities(
                        dag=dag,
                        method=method,
                        discharge_type=discharge_type,
                        year=year,
                        type_community_detection=type_community_detection)

                    op_community_detection.set_upstream(op_network_build)

                    op_plot_map = self.get_operator_plot_map(
                        dag=dag,
                        method=method,
                        discharge_type=discharge_type,
                        year=year,
                        type_community_detection=type_community_detection)

                    op_plot_map.set_upstream(op_community_detection)

                    for metric in MetricsSharedCareAreaFactory.TYPES:
                        op_sca_metrics = self.get_operator_sca_metrics(
                            dag=dag,
                            method=method,
                            discharge_type=discharge_type,
                            year=year,
                            type_community_detection=type_community_detection,
                            metric=metric
                        )
                        op_sca_metrics.set_upstream(op_community_detection)

        return dag

    def build_dag(self):
        list_methods = NetworkSharedCare.LIST_TYPES
        params = list_methods
        for method in params:
            dag = self.__create_dag__(method)
            globals()[dag.dag_id] = dag


factory = SharedCareAreaFactoryDag()
factory.build_dag()
