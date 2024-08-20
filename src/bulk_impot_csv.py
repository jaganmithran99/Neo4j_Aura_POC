import time

from neo4j import GraphDatabase


class BulkImportTopologyWithRelationsService:

    def __init__(self):
        self._node_label = "CI_2labels"
        self._unique_properties = ["assetId"]
        self._driver = self._establish_connection()

    @staticmethod
    def _establish_connection():
        neo4j_uname = "neo4j"  # replace with correct value
        neo4j_pwd = "pwd"
        neo4j_uri = "uri"

        if neo4j_uname and neo4j_pwd and neo4j_uri:
            start = time.time()
            _driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_uname, neo4j_pwd))
            end = time.time()
            print("Time taken for initializing driver: " + str(end - start))
            return _driver
        else:
            raise RuntimeError("Driver not initialized")

    def bulk_import(self, device_file_path, rel_file_path):
        with self._driver.session(database="neo4j") as session:
            self._create_constraint(session)
            self.import_nodes_n_rel(session, device_file_path, rel_file_path)
            session.close()
        self._driver.close()

    @staticmethod
    def _generate_field_mappings():
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
        if not mappings:
            raise ValueError("topologyDeviceFieldMappings not mapped")
        mapped_string = ", ".join(
            [f"a.{aiops_key}= coalesce(line['{ip_key}'], '')" for aiops_key, ip_key in mappings.items()])
        return mapped_string

    def import_nodes_n_rel(self, imp_session, n_file, rel_file):

        node_query = f"""
            LOAD CSV WITH HEADERS FROM $nodeFile AS line
            CALL {{
                WITH line
                MERGE (a:{self._node_label} {{assetId: line['Asset ID']}})
                SET {self._generate_field_mappings()}, a.internalAssetId = apoc.create.uuid()
                WITH a, line
                CALL apoc.create.addLabels(a, [line['Type']]) YIELD node
                RETURN node
            }}
            RETURN COUNT(*);
        """

        relationship_query = f"""
            LOAD CSV WITH HEADERS FROM $relationshipFile as line
            CALL {{
                WITH line
                MATCH (a1:{self._node_label} {{assetId: line['Source Asset ID']}})
                MATCH (a2:{self._node_label} {{assetId: line['Target Asset ID']}}) WITH a1,a2, line CALL 
                apoc.create.relationship(a1, line['Relationship Type Name'], apoc.map.removeKeys(line, ['Source Asset 
                ID', 'Target Asset ID', 'Relationship Type Name']), a2) YIELD rel RETURN COUNT(*) as count }} 
                IN TRANSACTIONS OF 10000 rows 
                RETURN count;"""

        node_start = time.time()
        result = imp_session.run(
            node_query,
            nodeFile=n_file
        )
        node_end = time.time()
        print("Time taken for importing nodes: " + str(node_end - node_start))

        rel_start = time.time()
        result = imp_session.run(
            relationship_query,
            relationshipFile=rel_file
        )
        rel_end = time.time()
        print("Time taken for importing relations: " + str(rel_end - rel_start))

    def _create_constraint(self, session):
        cypher_query = (
            f"CREATE CONSTRAINT {self._node_label}UniqueConstraints IF NOT EXISTS FOR (label:{self._node_label}) "
            f"REQUIRE ({', '.join(f'label.{prop}' for prop in self._unique_properties)}) IS NODE KEY"
        )
        session.run(cypher_query)


if __name__ == "__main__":
    device_details_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/artifacts/device_details_csv.csv"
    relationship_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/artifacts/relationship_details_csv.csv"
    obj = BulkImportTopologyWithRelationsService()
    obj.bulk_import(device_file_path=device_details_location, rel_file_path=relationship_location)
