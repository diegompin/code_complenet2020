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
from mhs.scripts.acquisition_script.chhs.healthcare_facility_listing_chhs_acquisition_script import CHHSFacilityListingAcquisitionScript


class CHHSFacilityListingAcquisitionFactoryDag(AcquisitionFactoryDag):

    def get_script(self,  **kwargs):
        return CHHSFacilityListingAcquisitionScript()

    def get_params(self):
        return {
            'config_class': 'CHHS',
            'config_name': 'HEALTHCARE_FACILITY_LISTING'
        }


factory = CHHSFacilityListingAcquisitionFactoryDag()
dag = factory.build_dag()
