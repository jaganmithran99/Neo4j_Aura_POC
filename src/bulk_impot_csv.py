import time
import uuid

from neo4j import GraphDatabase, WRITE_ACCESS


class BulkImportTopologyWithRelationsService:

    def __init__(self, *, project_id: int):
        self._project_id: int = int(project_id)
        self._node_label = "CI_100K_new"
        self._unique_properties = ["internalAssetId", "assetId"]
        self._driver = self._establish_connection()

    @staticmethod
    def _establish_connection():
        neo4j_uname = "neo4j"  # replace with correct value
        neo4j_pwd = "password"
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
        start = time.time()
        with self._driver.session(default_access_mode=WRITE_ACCESS) as session:
            self._create_constraint(session)
            n_start = time.time()
            # session.execute_write(self._import_nodes, device_file_path)
            self._run_import_nodes(session, device_file_path)
            n_end = time.time()
            print("Time taken to bulk import nodes: " + str(n_end - n_start))
            r_start = time.time()
            # session.execute_write(self._import_relationship, rel_file_path)
            self._run_import_relationship(session, rel_file_path)
            r_end = time.time()
            print("Time taken to bulk import relations: " + str(r_end - r_start))
            session.close()
        self._driver.close()

        end = time.time()
        print("BulkImportTopologyWithRelationsService Total Timing: " + str(end - start))
        msg = f"Bulk import successfully in Neo4j database {self._node_label}."
        return {"statusCode": 200, "statusMessage": msg}

    # def _import_nodes(self, tx, file_path):
    #     tx.run(
    #         "LOAD CSV WITH HEADERS FROM $file AS row "
    #         f"MERGE (n:{self._node_label} {{{self._generate_field_mappings()}}}) "
    #         "ON CREATE SET n += row, n.internalAssetId = $internalAssetId",
    #         file=f"{file_path}", internalAssetId=str(uuid.uuid4())
    #     )

    def _run_import_nodes(self, session, file_path):
        session.run(
            "LOAD CSV WITH HEADERS FROM $file AS row "
            f"CALL {{WITH row MERGE (n:{self._node_label} {{assetId: row.`Asset ID`}}) "
            f"SET {self._generate_field_mappings()}, n.internalAssetId = $internalAssetId}} IN TRANSACTIONS OF 10000 rows",
            file=f"{file_path}", internalAssetId=str(uuid.uuid4())
        )

    # def _import_relationship(self, tx, file_path):
    #     tx.run(
    #         "LOAD CSV WITH HEADERS FROM $file AS row "
    #         f"MATCH (source:{self._node_label} {{assetId: row.`Source Asset ID`}}) "
    #         f"MATCH (target:{self._node_label} {{assetId: row.`Target Asset ID`}}) "
    #         "CALL apoc.create.relationship(source, row.`Relationship Type Name`, apoc.map.removeKeys(row, ['Source Asset ID', 'Target Asset ID', 'Relationship Type Name']), target) YIELD rel "
    #         "RETURN rel",
    #         file=f"{file_path}"
    #     )

    def _run_import_relationship(self, session, file_path):
        session.run(
            "LOAD CSV WITH HEADERS FROM $file AS row "
            "CALL {"
            "WITH row "
            f"MATCH (source:{self._node_label} {{assetId: row.`Source Asset ID`}}) "
            f"MATCH (target:{self._node_label} {{assetId: row.`Target Asset ID`}}) "
            "CALL apoc.create.relationship(source, row.`Relationship Type Name`, apoc.map.removeKeys(row, ['Source Asset ID', 'Target Asset ID', 'Relationship Type Name']), target) YIELD rel "
            "RETURN count(rel) AS rel_count } "
            "IN TRANSACTIONS OF 10000 rows "
            "RETURN sum(rel_count) AS totalRelCount",
            file=f"{file_path}"
        )

    def _create_constraint(self, session):
        cypher_query = (
            f"CREATE CONSTRAINT IF NOT EXISTS FOR (label:{self._node_label}) "
            f"REQUIRE ({', '.join(f'label.{prop}' for prop in self._unique_properties)}) IS NODE KEY"
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
            [f"n.{aiops_key}= coalesce(row.`{ip_key}`, '')" for aiops_key, ip_key in mappings.items()])
        return mapped_string


if __name__ == "__main__":
    project_id = 60
    # device_details_location = "https://storagesamplecreateneo4j.blob.core.windows.net/container/device_details_100k_csv.csv?sp=r&st=2024-07-22T08:36:27Z&se=2024-08-14T16:36:27Z&spr=https&sv=2022-11-02&sr=b&sig=7FqRxZQvARoMT6PqFhcRGQNW07FqtXNiy70q%2Bom%2FFgE%3D"
    device_details_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/artifacts/100k/device_details_100k_csv.csv"
    # relationship_location = "https://storagesamplecreateneo4j.blob.core.windows.net/container/relationships_100k_csv.csv?sp=r&st=2024-07-22T08:40:43Z&se=2024-08-14T16:40:43Z&spr=https&sv=2022-11-02&sr=b&sig=HoUcPg3Y94X6xTFePe3gurDMwxYneI1OB8Duz8Bzqe4%3D"
    relationship_location = "https://raw.githubusercontent.com/jaganmithran99/Neo4j_Aura_POC/main/artifacts/100k/relationships_100k_csv.csv"
    obj = BulkImportTopologyWithRelationsService(project_id=project_id)
    response = obj.bulk_import(device_file_path=device_details_location, rel_file_path=relationship_location)
