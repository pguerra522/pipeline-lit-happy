#import all necessary packages
from datetime import datetime
from airflow.models import DAG
from pandas import DataFrame
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table

#Declare the connection paths
S3_FILE_PATH = "s3://pguerra522-pipeline-lit-happy"
S3_CONN_ID = "aws_default"
SNOWFLAKE_CONN_ID = "snowflake_default"

#Set the snowflake table name
SNOWFLAKE_REPORTING = "lit_reporting_table"


#sql query to join the happiness avg and the literacy info
#used a full join to avoid countries with no info on either side
@aql.transform
def join_happy_lit(lit: Table, happy: Table):
    return """SELECT l.country, l.literacy_rate, l.literacy_year, h.score
    FROM {{lit}} l JOIN {{happy}} h
    ON l.country = h.country"""

#calculate happiness score average between 2019 and 2018 of each country
@aql.transform
def avg_happy_data(table_2018: Table,table_2019: Table):
    return """SELECT COUNTRY, AVG(Score) as SCORE FROM(
        SELECT COUNTRY, SCORE FROM {{table_2018}}
        UNION ALL
        SELECT COUNTRY, SCORE FROM {{table_2019}}
    ) s
    Group by COUNTRY"""


#Dag setup sheduled daily
with DAG(
    dag_id="literacy_happiness_dag",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
):
    #Load country literacy rate dataset
    lit_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/lit_cl.csv", conn_id=S3_CONN_ID
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID
        )
    )

    #Load 2018 country happiness index dataset
    happy_2018_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/2018_cl.csv", conn_id=S3_CONN_ID
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID
        )
    )

    #Load 2019 country happiness index dataset
    happy_2019_data = aql.load_file(
        input_file=File(
            path=S3_FILE_PATH + "/2019_cl.csv", conn_id=S3_CONN_ID
        ),
        output_table=Table(
            conn_id=SNOWFLAKE_CONN_ID
        )
    )

    #call the happiness average transformation
    happy_avg = avg_happy_data(happy_2018_data,happy_2019_data)
    #call the literacy and happiness join transformation
    joined_data = join_happy_lit(lit_data, happy_avg)


    #merge data into snowflake
    lit_reporting_table = aql.merge(
        target_table=Table(
            name=SNOWFLAKE_REPORTING,
            conn_id=SNOWFLAKE_CONN_ID,
        ),
        target_conflict_columns=["country"],
        columns=["country","literacy_rate", "literacy_year","score"],
        if_conflicts="update",
        source_table=joined_data,
    ) >> aql.cleanup()
