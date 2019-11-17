# MHS
Mapping Healthcare Systems
Complex Care Lab
Heart Failure Program
University of California, Davis



## Prerequisites 

1) Install Docker https://docs.docker.com
2) Install docker-compose 
## Installation

1) Download code at https://github.com/diegompin/code_complenet2020
2) Download data at https://osf.io/gw73y/
3) Export system variable MHS_DATALINK to the downloaded data
4) Run script_init_00_docker.sh
5) Run script_init_01_services.sh
6) Run script_init_02_mongo.sh
7) Open http://localhost:8080 to see list of DAGs
![List of DAGs](/DAG_LIST.png)
8) Execute DAG_SHARED_CARE_AREA_HU for Comparative Analysis
![DAG HU](/DAG_HU.png)