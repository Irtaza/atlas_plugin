from datetime import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from atlas_plugin.operators.atlas_operators import AtlasCreateEntitiesBulkOperator

ATLAS_CONN_ID = 'atlas_default'

entity_dict = {"entities": [{
    "attributes": {"qualifiedName": "dcm_matchtable_placements_2019_03_17.csv",
                   "name": "dcm_matchtable_placements_2019_03_17.csv",
                   "path": "/tenant10/subtenant101/dcm/matchtables/"},
    "status": "ACTIVE",
    "version": 1,
    "typeName": "hdfs_path"}],
    "referredEntities": {}
}

dag = DAG('atlas_test', description='Apache Atlas Test DAG',
          schedule_interval='0 12 * * *',
          start_date=datetime(2019, 11, 20), catchup=False)

dummy_operator = DummyOperator(task_id='dummy_task', retries=3, dag=dag)

entity_operator = AtlasCreateEntitiesBulkOperator(task_id='create_entity_task',
                                                  atlas_conn_id=ATLAS_CONN_ID,
                                                  atlas_entity_dict=entity_dict, dag=dag)

dummy_operator >> entity_operator
