import duckdb
import dlt
from dlt.sources.helpers.rest_client import RESTClient
from dlt.sources.helpers.rest_client.paginators import PageNumberPaginator

# Define the API resource for NYC taxi data
@dlt.resource(name="ny_taxi_resource")   # <--- The name of the resource (will be used as the table name)
def ny_taxi():
    client = RESTClient(
        base_url="https://us-central1-dlthub-analytics.cloudfunctions.net/data_engineering_zoomcamp_api",
        paginator=PageNumberPaginator(
            base_page=1,
            total_path=None
        )
    )

    for page in client.paginate("data_engineering_zoomcamp_api"):    # <--- API endpoint for retrieving taxi ride data
        yield page   # <--- yield data to manage memory

# define new dlt pipeline
pipeline = dlt.pipeline(
    pipeline_name="ny_taxi_pipeline",
    destination="duckdb",
    dataset_name="ny_taxi_data"
)

# run the pipeline with the new resource
load_info = pipeline.run(ny_taxi, write_disposition="replace")
print(load_info)

# explore loaded data
df = pipeline.dataset(dataset_type="default").ny_taxi_resource.df()
print(df)

# A database '<pipeline_name>.duckdb' was created in working directory so just connect to it
# Connect to the DuckDB database
conn = duckdb.connect(f"{pipeline.pipeline_name}.duckdb")

# Set search path to the dataset
conn.sql(f"SET search_path = '{pipeline.dataset_name}'")

# Describe the dataset
print(conn.sql("DESCRIBE").df())

with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT COUNT(0)
            FROM ny_taxi_resource;
            """
        )
    print(res)

with pipeline.sql_client() as client:
    res = client.execute_sql(
            """
            SELECT
            AVG(date_diff('minute', trip_pickup_date_time, trip_dropoff_date_time))
            FROM ny_taxi_resource;
            """
        )
    print('Trip duration')
    # Prints column values of the first row
    print(res)