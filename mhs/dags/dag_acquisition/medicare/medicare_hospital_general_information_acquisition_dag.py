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
from mhs.scripts.acquisition_script.medicare.hospital_information_medicare_acquisition_script import \
    MedicareHospitalGeneralInformationAcquisitionScript


class MedicareHospitalGeneralInformationAcquisitionFactoryDag(AcquisitionFactoryDag):

    def get_script(self,  **kwargs):
        return MedicareHospitalGeneralInformationAcquisitionScript()

    def get_params(self):
        return {
            'config_class': 'MEDICARE',
            'config_name': 'HOSPITAL_GENERAL_INFORMATION'
        }


factory = MedicareHospitalGeneralInformationAcquisitionFactoryDag()
dag = factory.build_dag()
