import time

from neo4j import GraphDatabase


class BulkImportTopologyWithRelationsService:

    def __init__(self, *, project_id: int):
        self._project_id: int = int(project_id)
        self._node_label = "BI"
        self._unique_properties = ["smartopsAssetId", "assetId"]
        self._driver = self._establish_connection()

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

    def bulk_import(self, device_file_path, rel_file_path):
        start = time.time()
        with self._driver.session() as session:
            self._create_constraint(session)
            n_start = time.time()
            session.execute_write(self._import_nodes, device_file_path)
            n_end = time.time()
            print("Time taken to bulk import nodes: " + str(n_end - n_start))
            r_start = time.time()
            session.execute_write(self._import_relationship, rel_file_path)
            r_end = time.time()
            print("Time taken to bulk import relations: " + str(r_end - r_start))
            self._driver.close()

        end = time.time()
        print("BulkImportTopologyWithRelationsService Total Timing: " + str(end - start))
        msg = f"Bulk import successfully in Neo4j database {self._node_label}."
        return {"statusCode": 200, "statusMessage": msg}

    def _import_nodes(self, tx, file_path):
        tx.run(
            "LOAD CSV WITH HEADERS FROM $file AS row "
            f"MERGE (n:{self._node_label} {{{self._generate_field_mappings()}}}) "
            "ON CREATE SET n += row",
            file=f"{file_path}"
        )

    def _import_relationship(self, tx, file_path):
        tx.run(
            "LOAD CSV WITH HEADERS FROM $file AS row "
            f"MATCH (source:{self._node_label} {{assetId: row.`Source Asset ID`}}) "
            f"MATCH (target:{self._node_label} {{assetId: row.`Target Asset ID`}}) "
            "MERGE (source)-[r:`Relationship Type Name`]->(target) "
            "SET r = row "
            "REMOVE r.`Source Asset ID`, r.`Target Asset ID`",
            file=f"{file_path}"
        )

    def _create_constraint(self, session):
        cypher_query = (
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (label:{self._node_label}) "
            f"REQUIRE ({', '.join(f'label.{prop}' for prop in self._unique_properties)}) IS UNIQUE"
        )
        session.run(cypher_query)

    @staticmethod
    def _generate_field_mappings():
        mappings = {
            "assetId": "Asset ID",
            "assetName": "Asset Name",
            "type": "Type",
            "description": "Description",
            "ipAddress": "IP Address",
            "macAddress": "MA\nC Address",
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
            [f"{aiops_key}: coalesce(row.`{ip_key}`, '')" for aiops_key, ip_key in mappings.items()])
        return mapped_string


if __name__ == "__main__":
    project_id = 60
    device_details_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/device_details_csv.csv"
    relationship_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/relationship_details_csv.csv"
    obj = BulkImportTopologyWithRelationsService(project_id=project_id)
    response = obj.bulk_import(device_file_path=device_details_location, rel_file_path=relationship_location)
