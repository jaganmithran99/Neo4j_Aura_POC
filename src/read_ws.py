import time
import traceback

from neo4j import GraphDatabase

logger_tag = "[RETRIEVE-NODE-RELATIONS] "


class RetrieveNodeAndRelations(object):

    def __init__(self, node_name: str, project_id: int, relationship_types: list[dict], direction: str,
                 relation_levels, limit) -> None:
        self.project_id: int = project_id
        self.node_name: str = node_name
        self.relationship_types: list[dict] = relationship_types
        self.direction: str = direction
        self.limit: int = limit
        self.relation_levels = relation_levels
        self.node_and_its_relations: list = []
        self.related_node_name_list: list = []
        self.driver = self._establish_connection()

    @staticmethod
    def _establish_connection():
        neo4j_uname = "neo4j"
        neo4j_pwd = "WdI0iz8218Vd-0IN4PRRMGhp5dGOvbGvNKdxCmXAvT4"
        neo4j_uri = "neo4j+ssc://758e9445.databases.neo4j.io"

        if neo4j_uname and neo4j_pwd and neo4j_uri:
            start = time.time()
            _driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_uname, neo4j_pwd))
            end = time.time()
            print("Time taken for initializing driver: " + str(end - start))
            return _driver
        else:
            raise RuntimeError("Driver not initialized")

    def _build_query(self, relationship_type, direction, relation_level):
        if direction.lower() == "incoming":
            query = (
                f"MATCH p = (node:CI)<-[r*1..{relation_level}]-(related) "
                f"WHERE node.name = $assetName AND ALL(rel IN r WHERE type(rel) = '{relationship_type}') "
                "WITH node, relationships(p) AS rels, related "
                f"LIMIT {self.limit} "
                "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
            )
        elif direction.lower() == "outgoing":
            query = (
                f"MATCH p = (node:CI)-[r*1..{relation_level}]->(related) "
                f"WHERE node.name = $assetName AND ALL(rel IN r WHERE type(rel) = '{relationship_type}') "
                "WITH node, relationships(p) AS rels, related "
                f"LIMIT {self.limit} "
                "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
            )
        else:
            query = (
                f"MATCH p = (node:CI)-[r*1..{relation_level}]-(related) "
                f"WHERE node.name = $assetName AND ALL(rel IN r WHERE type(rel) = '{relationship_type}') "
                "WITH node, relationships(p) AS rels, related "
                f"LIMIT {self.limit} "
                "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
            )
        return query

    def _run_query_and_format_data(self, query, session):
        start_time = time.time()

        def _run_txn(tx):
            result = tx.run(query, assetName=self.node_name, relationship_types=self.relationship_types)
            return [rcd for rcd in result]

        # node_relations = session.run(query, assetName=self.node_name, relationship_types=self.relationship_types)
        node_relations = session.execute_read(_run_txn)
        print(f"{node_relations=}")
        end_time = time.time()
        print('execution_time', end_time - start_time)
        for record in node_relations:
            data = record.get('data', [])
            self.node_and_its_relations.extend(data)

    def retrieve_relation_using_node_name(self) -> dict:
        try:
            start_time = time.time()
            with self.driver.session() as session:
                if self.relationship_types:
                    for types in self.relationship_types:
                        relationship_type = types.get("relation", "")
                        relation_level = types.get("relationLevel", "")
                        direction = types.get("direction", "")
                        query = self._build_query(relationship_type, direction, relation_level)
                        self._run_query_and_format_data(query, session)
                else:
                    if self.direction.lower() == "incoming":
                        '''query to fetch incoming relations'''
                        query = (
                            f"MATCH p = (node:CI)<-[r*1..{self.relation_levels}]-(related) "
                            f"WHERE node.name = $assetName "
                            "WITH node, relationships(p) AS rels, related "
                            f"LIMIT {self.limit} "
                            "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
                        )
                        self._run_query_and_format_data(query, session)
                    elif self.direction.lower() == "outgoing":
                        '''query to fetch outgoing relations'''
                        query = (
                            f"MATCH p = (node:CI)-[r*1..{self.relation_levels}]->(related) "
                            f"WHERE node.name = $assetName "
                            "WITH node, relationships(p) AS rels, related "
                            f"LIMIT {self.limit} "
                            "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
                        )
                        self._run_query_and_format_data(query, session)
                    else:
                        ''' query to fetch both incoming and outgoing relations '''
                        query = (
                            f"MATCH p = (node:CI)-[r*1..{self.relation_levels}]-(related) "
                            f"WHERE node.name = $assetName "
                            "WITH node, relationships(p) AS rels, related "
                            f"LIMIT {self.limit} "
                            "RETURN COLLECT({relatedNode: related.name, positionNumber: size(rels), relation: type(rels[size(rels) - 1]), relatedNodeProperties: properties(related)}) AS data"
                        )
                        self._run_query_and_format_data(query, session)
            end_time = time.time()
            print('final execution_time', end_time - start_time)
            print(self.node_and_its_relations)

        except Exception as exc:
            print(f'{logger_tag} exception occurred {exc}')
            print(traceback.format_exc())

        finally:
            self.driver.close()


if __name__ == "__main__":
    node_name = "Device7"
    project_id = 60
    relationship_types = []
    direction = ""
    relation_levels = 1
    limit = 500
    obj = RetrieveNodeAndRelations(node_name, project_id, relationship_types,
                                   direction, relation_levels, limit)
    return_data = obj.retrieve_relation_using_node_name()
