__author__ = 'diegopinheiro'
__email__ = 'diegompin@gmail.com'
__github__ = 'https://github.com/diegompin'


import os
from airflow.models import DagBag

def package_files(directory):
    paths = []
    for dir in os.listdir(directory):
        curr_path = f'{directory}/{dir}'
        if not os.path.isdir(curr_path):
            continue
        if '__' in curr_path:
            continue
        paths.append(curr_path)
    return paths


for dir in package_files('./mhs/dags'):
   dag_bag = DagBag(os.path.expanduser(dir))

   if dag_bag:
      for dag_id, dag in dag_bag.dags.items():
         globals()[dag_id] = dag
