# Plugin - Apache Atlas

This plugin is used to create typeDefs, entites, entity instances in Apache Atlas. The pligin also has support for searching capabilities of Apache Atlas.

## Creating a connection to Apache Atlas
To create a connection with Atlas using the Airflow UI you need to open the interface > Admin dropdown menu > click on "connections" > create. Create an HTTP connection with the name "atlas_default". Put in the URL of your Apache Atlas instance, port, username and password.

## Installation
This plugin requires the [atlasclient](https://github.com/jpoullet2000/atlasclient/) module. This module has to be installed on your Airflow instance.
``` 
pip install atlasclient
```

## Hooks
### AtlasHook
This hook handles the authentication and request to Apache Atlas. 


## Operators
### AtlasCreateTypeDefOperator
To create a typeDef, one needs to create a Python dictionary which will define the typeDef and then pass it to the operator:
 
 ```   
    
typedef_operator = AtlasCreateTypeDefOperator(task_id='create_typeDef_task',
	atlas_conn_id = ATLAS_CONN_ID,
	atlas_typedef_dict=typedef_dict, dag=dag)

```

Here is an example of the typedef_dict Python dictionary to use with the operator:

```

  "enumDefs": [],
  "structDefs": [],
  "classificationDefs": [
    {
      "category": "CLASSIFICATION",
      "name": "Processed",
      "description": "Used for classifying data that has been processed.",
      "typeVersion": "1.0",
      "attributeDefs": [],
      "superTypes": []
    }
  ],{
  "entityDefs": []
}
```  

### AtlasCreateEntityOperator
To create an entity, one needs to create a Python dictionary which will define the entity and then pass it to the operator.
 
 ```   
    entity_operator = AtlasCreateEntitiesBulkOperator(task_id='create_entity_task',
	atlas_conn_id = ATLAS_CONN_ID,
	atlas_entity_dict=entity_dict, dag=dag)
```

Here is an example of the entity_dict Python dictionary to use with the operator:

```
{
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
```

### AtlasSearchByAttributes
To search for entities with a special attribute name 
 ```   
    entity_operator = AtlasSearchByAttributes(task_id='attribute_search_task',
	atlas_conn_id = ATLAS_CONN_ID,
	atlas_search_dict=search_dict, dag=dag)
```

Here is an example of the entity_dict Python dictionary to use with the operator:

```
search_dict = {'typeName': 'DataSet', 'attrName': 'name', 'attrValue': 'data', 
                    'offset': '1', 'limit': '10'}
```