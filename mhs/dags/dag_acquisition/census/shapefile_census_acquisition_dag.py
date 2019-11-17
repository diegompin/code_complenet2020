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
from mhs.scripts.acquisition_script.census.shapefile_acquisition_census_acquisition_script import CensusShapefileScript


class ShapefileCensusAcquisitionDagFactory(AcquisitionFactoryDag):

    def __init__(self):
        super().__init__()

    def get_params(self):
        return {
            'config_class': "CENSUS",
            'config_name': "SHAPEFILE"
        }

    def get_script(self,  **kwargs):
        return CensusShapefileScript()


factory = ShapefileCensusAcquisitionDagFactory()
dag = factory.build_dag()

# dag.tasks[2].execute_callable()
