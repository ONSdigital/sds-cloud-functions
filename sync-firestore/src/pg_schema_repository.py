import sqlalchemy
from google.cloud.sql.connector import Connector, IPTypes

def insert_schema_metrics(survey_id, title, schema_version, sds_schema_version, sds_published_at):
    # initialize Python Connector object
    connector = Connector()

    # Python Connector database connection function
    def getconn():
        conn = connector.connect(
            "ons-cir-sandbox-384314:europe-west2:testing-sds-metrics", # Cloud SQL Instance Connection Name
            "pg8000",
            user="postgres",
            password="sds123",
            db="sds-metrics",
            ip_type= IPTypes.PUBLIC  # IPTypes.PRIVATE for private IP
        )
        return conn

    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    # interact with Cloud SQL database using connection pool
    with pool.connect() as db_conn:
        # insert statement
        insert_stmt = sqlalchemy.text(
            "INSERT INTO \"sds-schema-metrics\" (survey_id, title, schema_version, sds_schema_version, sds_published_at) VALUES (:survey_id, :title, :schema_version, :sds_schema_version, :sds_published_at)",
        )
        # insert into database
        db_conn.execute(insert_stmt, parameters={"survey_id": survey_id, "title": title, "schema_version": schema_version, "sds_schema_version": sds_schema_version, "sds_published_at": sds_published_at})

        # commit transaction (SQLAlchemy v2.X.X is commit as you go)
        db_conn.commit()

def query_schema_metrics(survey_id, sds_schema_version) -> int:
    # initialize Python Connector object
    connector = Connector()

    # Python Connector database connection function
    def getconn():
        conn = connector.connect(
            "ons-cir-sandbox-384314:europe-west2:testing-sds-metrics", # Cloud SQL Instance Connection Name
            "pg8000",
            user="postgres",
            password="sds123",
            db="sds-metrics",
            ip_type= IPTypes.PUBLIC  # IPTypes.PRIVATE for private IP
        )
        return conn

    # create connection pool with 'creator' argument to our connection object function
    pool = sqlalchemy.create_engine(
        "postgresql+pg8000://",
        creator=getconn,
    )

    # interact with Cloud SQL database using connection pool
    with pool.connect() as db_conn:
        # query statement
        query_stmt = sqlalchemy.text(
            "SELECT COUNT(*) FROM \"sds-schema-metrics\" WHERE survey_id = :survey_id AND sds_schema_version = :sds_schema_version",
        )
        # query from database
        result = db_conn.execute(query_stmt, parameters={"survey_id": survey_id, "sds_schema_version": sds_schema_version})

        count = result.scalar_one()
        
        return count