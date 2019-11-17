__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'
'''
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
!!!!! THIS SECTION IS NECESSARY FOR AIRFLOW TO RECOGNIZE THE DAG !!!!! 
from airflow import DAG
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! 
'''
from mhs.dags.dag_acquisition.aquisition_factory_dag import AcquisitionFactoryDag
from mhs.scripts.acquisition_script.chhs.patient_origin_market_share_chhs_acquisition_script import CHHSPatientOriginMarketShareAcquisitionScript


class CHHSPatientOriginMarketShareAcquisitionFactoryDag(AcquisitionFactoryDag):

    def get_script(self, **kwargs):
        return CHHSPatientOriginMarketShareAcquisitionScript.instance(**kwargs)

    def get_params(self):
        return {
            'config_class': 'CHHS',
            'config_name': 'PATIENT_ORIGIN_MARKET_SHARE'
        }


factory = CHHSPatientOriginMarketShareAcquisitionFactoryDag()
dag = factory.build_dag()
