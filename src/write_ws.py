import time
import uuid
from typing import List

import pandas as pd
from neo4j import GraphDatabase


class ImportTopologyWithRelationsService:
    def __init__(self, *, project_id: int, device_details_data: List[dict], relationship_data: List[dict]):
        self._project_id: int = int(project_id)
        self._device_details_data: List[dict] = device_details_data
        self._relationship_data: List[dict] = relationship_data
        self._node_label = "CI"
        self._unique_properties = ["smartopsAssetId", "assetId"]
        self._node_indices = ["name"]
        self._rel_indices = ["type", "assetId"]
        self._driver = self._establish_connection()

    @staticmethod
    def _establish_connection():

        neo4j_uname = "neo4j"
        neo4j_pwd = "WdI0iz8218Vd-0IN4PRRMGhp5dGOvbGvNKdxCmXAvT4"
        neo4j_uri = "neo4j+s://758e9445.databases.neo4j.io"

        if neo4j_uname and neo4j_pwd and neo4j_uri:
            start = time.time()
            _driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_uname, neo4j_pwd))
            end = time.time()
            print("Time taken for initializing driver: " + str(end - start))
            return _driver
        else:
            raise RuntimeError("Driver not initialized")

    def process_input(self):
        start = time.time()
        with self._driver.session() as session:
            self._create_indices(session)
            self._create_constraints(session)
            self._create_nodes(session)
            self._match_relationships(session)
            session.close()
        self._driver.close()
        end = time.time()
        print("Total time taken for execution: " + str(end - start))
        msg = f"Nodes and relationships created successfully in Neo4j database {self._node_label}."
        return {"statusCode": 200, "statusMessage": msg}

    def _create_nodes(self, session):
        full_start = time.time()
        self._convert_to_aiops_fields()
        for item in self._device_details_data:
            asset_name = item['assetName']
            properties = {k: v for k, v in item.items() if k != 'assetName'}

            def _create_node_tx(tx, asset_name, properties):
                cypher_query = (
                    f"MERGE (n:{self._node_label} {{name: $asset_name}}) "
                    "SET n += $properties"
                )
                tx.run(cypher_query, asset_name=asset_name, properties=properties)

            start = time.time()
            # session.execute_write(_create_node_tx, asset_name=asset_name, properties=properties)
            _create_node_tx(session, asset_name=asset_name, properties=properties)
            end = time.time()
            print("Time taken for creating each nodes: " + str(end - start))
        full_end = time.time()
        print("Total Time taken for creating nodes: " + str(full_end - full_start))

    def _match_relationships(self, session):
        full_start = time.time()
        for item in self._relationship_data:
            source_asset_id = item['Source Asset ID']
            target_asset_id = item['Target Asset ID']
            rel_type_name = item['Relationship Type Name']
            properties = {k: v for k, v in item.items() if
                          k not in ['Relationship Type Name', 'Source Asset ID', 'Target Asset ID']}

            def _match_rel_tx(tx, rel_type_name, source_asset_id, target_asset_id, properties):
                cypher_query = (
                        f"MATCH (source:{self._node_label} {{assetId: $source_asset_id}}), (target:{self._node_label} {{assetId: $target_asset_id}}) "
                        "MERGE (source)-[r:`" + rel_type_name + "`]->(target) "
                                                                "SET r += $properties"
                )
                tx.run(cypher_query, source_asset_id=source_asset_id, target_asset_id=target_asset_id,
                       properties=properties)

            start = time.time()
            # session.execute_write(_match_rel_tx, rel_type_name=rel_type_name, source_asset_id=source_asset_id, target_asset_id=target_asset_id,
            #                       properties=properties)
            _match_rel_tx(session, rel_type_name=rel_type_name, source_asset_id=source_asset_id,
                          target_asset_id=target_asset_id,
                          properties=properties)
            end = time.time()
            print("Time taken for matching each nodes: " + str(end - start))
        full_end = time.time()
        print("Total Time taken for matching relations: " + str(full_end - full_start))

    def _convert_to_aiops_fields(self):
        mappings = {
            "assetId": "Asset ID",
            "assetName": "Asset Name",
            "type": "Type",
            "description": "Description",
            "ipAddress": "IP Address",
            "macAddress": "MAC Address",
            "serialNumber": "Serial Number",
            "modelNuber": "Model Number",
            "deviceStatus": "Status",
            "decommissioned": "Decommissioned",
            "businessCriticality": "Business Criticality",
            "impactRadius": "Impact Radius",
            "resourceGroup": "Resource Group",
            "vendor": "Vendor",
            "manufacturer": "Manufacturer",
            "deviceContact": "Device Contact",
            "country": "Country",
            "site": "Site",
            "region": "Region",
            "businessTimeZone": "Business Time Zone",
            "tags": "Tags"
        }
        converted_list = []
        for item in self._device_details_data:
            converted_item = {}
            for aiops_key, ip_key in mappings.items():
                if ip_key in item:
                    converted_item[aiops_key] = item[ip_key]
            converted_item["smartopsAssetId"] = str(uuid.uuid4())
            converted_list.append(converted_item)

        self._device_details_data = converted_list

    def _create_indices(self, session):
        start = time.time()

        def _create_indices_tx(tx):
            node_index_query = f"CREATE INDEX composite_range_node_index_name IF NOT EXISTS FOR (n:{self._node_label}) ON ({', '.join(f'n.{index}' for index in self._node_indices)})"
            rel_index_query = f"CREATE INDEX composite_range_rel_index_name1 IF NOT EXISTS FOR ()-[r:{self._node_label}]-() ON ({', '.join(f'r.{index}' for index in self._rel_indices)})"
            tx.run(node_index_query)
            tx.run(rel_index_query)

        session.execute_write(_create_indices_tx)
        end = time.time()
        print("Time taken for creating indices: " + str(end - start))

    def _create_constraints(self, session):
        start = time.time()

        def _create_constraints_tx(tx):
            cypher_query = (
                f"CREATE CONSTRAINT IF NOT EXISTS FOR (label:{self._node_label}) "
                f"REQUIRE ({', '.join(f'label.{prop}' for prop in self._unique_properties)}) IS UNIQUE"
            )
            tx.run(cypher_query)

        session.execute_write(_create_constraints_tx)
        end = time.time()
        print("Time taken for creating constraints: " + str(end - start))


if __name__ == "__main__":
    device_details_location = "C:/Users/192296/Downloads/Neo4j/demo/device_details.xlsx"
    relationship_location = "C:/Users/192296/Downloads/Neo4j/demo/relationship_details.xlsx"
    device_details_df = pd.read_excel(device_details_location)
    relationship_df = pd.read_excel(relationship_location)
    device_details_data = device_details_df.to_dict(orient='records')
    relationship_data = relationship_df.to_dict(orient='records')
    obj = ImportTopologyWithRelationsService(project_id=60, device_details_data=device_details_data,
                                             relationship_data=relationship_data)
    response = obj.process_input()
