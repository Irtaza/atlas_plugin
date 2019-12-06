import logging

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from atlas_plugin.hooks.atlas_hook import AtlasHook

log = logging.getLogger(__name__)


class AtlasCreateTypeDefOperator(BaseOperator):
    """
    An operator to create typeDefs in Atlas type definitions, only new definitions will be created.
        Any changes to the existing definitions will be discarded
        This can include enumDefs, structDefs, entityDefs, classificationDefs
    :param atlas_conn_id: The Atlas connection of type HTTP defined in Airflow
    :type atlas_conn_id: str
    :param atlas_entity_dict: A Python dictionary that defines the typeDef
    :type atlas_entity_dict: dict
    **Example**:
        The following operator would create a new Classification type

        typedef_dict =   "classificationDefs":
        [
            {
              "category": "CLASSIFICATION",
              "name": "Processed",
              "description": "Used for classifying data that has been processed.",
              "typeVersion": "1.0",
              "attributeDefs": [],
              "superTypes": []
            }
        ]

        typedef_operator = AtlasCreateTypeDefOperator(task_id='create_typeDef_task',
	        atlas_conn_id = ATLAS_CONN_ID,
	        atlas_typedef_dict=typedef_dict, dag=dag)
    """
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self, atlas_conn_id, atlas_entity_dict, *args, **kwargs):
        self.atlas_conn_id = atlas_conn_id
        self.atlas_entity_dict = atlas_entity_dict
        super(AtlasCreateTypeDefOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Start: TypeDef Creation")
        log.info('atlas_entity_dict: %s', self.atlas_entity_dict)

        hook = AtlasHook(self.atlas_conn_id)
        response = hook.create_type(self.atlas_entity_dict)
        task_instance = context['task_instance']
        task_instance.xcom_push('entity_response', response)

        log.info("Stop: TypeDef Creation")


class AtlasCreateEntityOperator(BaseOperator):
    """
        An operator to create a new entity or update existing entity in Atlas.
        Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
        :param atlas_conn_id: The Atlas connection of type HTTP defined in Airflow
        :type atlas_conn_id: str
        :param atlas_entity_dict: A Python dictionary that defines the typeDef
        :type atlas_entity_dict: dict
        **Example**:
            The following operator would create a new entity of type hdfs_path

            entity_dict =  {
                  "entity": {
                    {
                      "attributes": {
                        "qualifiedName": "dcm_matchtable_placements_2019_03_17.csv",
                        "name": "dcm_matchtable_placements_2019_03_17.csv",
                        "path": "/tenant10/subtenant101/dcm/matchtables/"
                      },
                      "status": "ACTIVE",
                      "version": 1,
                      "typeName": "hdfs_path"
                    }
                  },
                  "referredEntities": {}
            }


            entity_operator = AtlasCreateEntityOperator(task_id='create_entity_task',
                atlas_conn_id = ATLAS_CONN_ID,
                atlas_entity_dict=entity_dict, dag=dag)
        """
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self, atlas_conn_id, atlas_entity_dict, *args, **kwargs):
        self.atlas_conn_id = atlas_conn_id
        self.atlas_entity_dict = atlas_entity_dict
        super(AtlasCreateEntityOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Start: Entity Creation")
        log.info('atlas_entity_dict: %s', self.atlas_entity_dict)

        hook = AtlasHook(self.atlas_conn_id)
        response = hook.create_entity(self.atlas_entity_dict)
        task_instance = context['task_instance']
        task_instance.xcom_push('entity_response', response)

        log.info("Stop: Entity Creation")


class AtlasCreateEntitiesBulkOperator(BaseOperator):
    """
        An operator to create new entities or update existing entities in Atlas.
        Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
        :param atlas_conn_id: The Atlas connection of type HTTP defined in Airflow
        :type atlas_conn_id: str
        :param atlas_entity_dict: A Python dictionary that defines the typeDef
        :type atlas_entity_dict: dict

        **Example**:
            The following operator would create new entities of type hdfs_path
            entity_dict =  {
                  "entities": [
                    {
                      "attributes": {
                        "qualifiedName": "dcm_matchtable_placements_2019_03_17.csv",
                        "name": "dcm_matchtable_placements_2019_03_17.csv",
                        "path": "/tenant10/subtenant101/dcm/matchtables/"
                      },
                      "status": "ACTIVE",
                      "version": 1,
                      "typeName": "hdfs_path"
                    }
                  ],
                  "referredEntities": {}
            }

            entity_operator = AtlasCreateEntitiesBulkOperator(task_id='create_entity_task',
                atlas_conn_id = ATLAS_CONN_ID,
                atlas_entity_dict=entity_dict, dag=dag)
        """
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self, atlas_conn_id, atlas_entity_dict, *args, **kwargs):
        self.atlas_conn_id = atlas_conn_id
        self.atlas_entity_dict = atlas_entity_dict
        super(AtlasCreateEntitiesBulkOperator, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Start: Bulk Entity Creation")
        log.info('atlas_entity_dict: %s', self.atlas_entity_dict)

        hook = AtlasHook(self.atlas_conn_id)
        response = hook.create_entity_bulk(self.atlas_entity_dict)
        task_instance = context['task_instance']
        task_instance.xcom_push('entities_response', response)

        log.info("Stop: Bulk Entity Creation")


class AtlasSearchByAttributes(BaseOperator):
    """
        An operator to create typeDefs in Atlas type definitions, only new definitions will be created.
            Any changes to the existing definitions will be discarded
            This can include enumDefs, structDefs, entityDefs, classificationDefs
        :param atlas_conn_id: The Atlas connection of type HTTP defined in Airflow
        :type atlas_conn_id: str
        :param atlas_entity_dict: A Python dictionary that defines the typeDef
        :type atlas_entity_dict: dict
        **Example**:
            The following operator would create a new Classification type

            typedef_dict =   "classificationDefs":
            [
                {
                  "category": "CLASSIFICATION",
                  "name": "Processed",
                  "description": "Used for classifying data that has been processed.",
                  "typeVersion": "1.0",
                  "attributeDefs": [],
                  "superTypes": []
                }
            ]

            typedef_operator = AtlasCreateTypeDefOperator(task_id='create_typeDef_task',
    	        atlas_conn_id = ATLAS_CONN_ID,
    	        atlas_typedef_dict=typedef_dict, dag=dag)
        """
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self, atlas_conn_id, atlas_search_dict, *args, **kwargs):
        self.atlas_conn_id = atlas_conn_id
        self.atlas_search_dict = atlas_search_dict
        super(AtlasSearchByAttributes, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Start: Attribute Search")
        log.info('atlas_entity_dict: %s', self.atlas_entity_dict)

        hook = AtlasHook(self.atlas_conn_id)
        response = hook.search_attributes(self.atlas_entity_dict)
        task_instance = context['task_instance']
        task_instance.xcom_push('search_attributes_response', response)

        log.info("Stop Attribute Search")


class AtlasSearchByDSL(BaseOperator):
    """
        An operator to create typeDefs in Atlas type definitions, only new definitions will be created.
            Any changes to the existing definitions will be discarded
            This can include enumDefs, structDefs, entityDefs, classificationDefs
        :param atlas_conn_id: The Atlas connection of type HTTP defined in Airflow
        :type atlas_conn_id: str
        :param atlas_entity_dict: A Python dictionary that defines the typeDef
        :type atlas_entity_dict: dict
        **Example**:
            The following operator would create a new Classification type

            typedef_dict =   "classificationDefs":
            [
                {
                  "category": "CLASSIFICATION",
                  "name": "Processed",
                  "description": "Used for classifying data that has been processed.",
                  "typeVersion": "1.0",
                  "attributeDefs": [],
                  "superTypes": []
                }
            ]

            typedef_operator = AtlasCreateTypeDefOperator(task_id='create_typeDef_task',
    	        atlas_conn_id = ATLAS_CONN_ID,
    	        atlas_typedef_dict=typedef_dict, dag=dag)
        """
    ui_color = '#ffd700'

    @apply_defaults
    def __init__(self, atlas_conn_id, atlas_search_dict, *args, **kwargs):
        self.atlas_conn_id = atlas_conn_id
        self.atlas_search_dict = atlas_search_dict
        super(AtlasSearchByDSL, self).__init__(*args, **kwargs)

    def execute(self, context):
        log.info("Start: DSL Search")
        log.info('atlas_entity_dict: %s', self.atlas_entity_dict)

        hook = AtlasHook(self.atlas_conn_id)
        response = hook.search_dsl(self.atlas_entity_dict)
        task_instance = context['task_instance']
        task_instance.xcom_push('search_dsl_response', response)

        log.info("Stop DSL Search")
