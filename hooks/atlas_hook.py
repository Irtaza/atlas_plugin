from airflow.hooks.base_hook import BaseHook
from atlasclient.client import Atlas


class AtlasHook(BaseHook):
    """
    Interact with Apache Atlas
    This class is a thin wrapper around the atlasclient pyhton library.
    """

    def __init__(self, conn_id):
        connection = HttpHook.get_connection(conn_id)
        self.host = connection.host
        self.port = connection.port
        self.username = connection.login
        self.password = connection.password
        self.client = Atlas(self.host, port=self.port, username=self.username,
                            password=self.password)

    def search_attributes(self, **kwargs):
        """
        Attribute based search for entities satisfying the search parameters.
        """
        entities_list = []
        search_results = self.client.search_attribute(**kwargs)

        for s in search_results:
            for e in s.entities:
                entities_list.append(e.attributes)

        return {"entities": entities_list}

    def search_dsl(self, **kwargs):
        """
        Retrieve data for the specified DSL
        """
        entities_list = []
        search_results = self.client.search_dsl(**kwargs)

        for s in search_results:
            for e in s.entities:
                entities_list.append(e.attributes)

        return {"entities": entities_list}

    def create_type(self, typedef_dict):
        """
        Create atlas type definitions, only new definitions will be created.
        Any changes to the existing definitions will be discarded
        This can include enumDefs, structDefs, entityDefs, classificationDefs
        """
        return self.client.typedefs.create(data=typedef_dict)

    def create_entity(self, typedef_dict):
        """
        Create a new entity or update existing entity in Atlas.
        Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
        """
        return self.client.entity_post.create(data=typedef_dict)

    def create_entity_bulk(self, typedef_dict):
        """
        Creates new entities or updates existing entities in Atlas.
        Existing entity is matched using its unique guid if supplied or by its unique attributes eg: qualifiedName
        """
        return self.client.entity_bulk.create(data=typedef_dict)
