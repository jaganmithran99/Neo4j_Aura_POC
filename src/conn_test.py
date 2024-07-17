from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable, AuthError

uri = "neo4j+ssc://758e9445.databases.neo4j.io"
username = "neo4j"
password = "WdI0iz8218Vd-0IN4PRRMGhp5dGOvbGvNKdxCmXAvT4"


def check_connection(uri, username, password):
    try:
        driver = GraphDatabase.driver(uri, auth=(username, password))
        with driver.session() as session:
            result = session.run("RETURN 'Connection Successful' AS message")
            for record in result:
                print(record["message"])
        driver.close()
    except ServiceUnavailable as e:
        print(f"ServiceUnavailable: {e}")
    except AuthError as e:
        print(f"AuthError: {e}")
    except Exception as e:
        print(f"An error occurred: {e}")


check_connection(uri, username, password)
