"""
If running in an AWS Glue Notebook, add the following Libraries under "Advanced Properties"

1. The Sedona 1.8.0, Spark 3.5, Scala 2.12 shaded JAR must be available on s3. Fetch the
   JAR from https://mvnrepository.com/artifact/org.apache.sedona/sedona-spark-shaded-3.5_2.12/1.8.0,
   upload it to your s3 bucket, then add the s3 URI to the `Dependent JARs path`
   Example: s3://<my bucket>/transportation-splitter/sedona-spark-shaded-3.5_2.12-1.8.0.jar

2. Add the following to `Additional Python modules path`:
   apache-sedona==1.8.0,shapely,pyproj

3. The Transportation Splitter library. Download or build the latest wheel for the Transportation Splitter
   and upload it to s3. Add it to the `Python library path`.
   Example: s3://<my bucket>/transportation-splitter/transportation_splitter-0.2.0-py3-none-any.whl

4. Use Glue Version 5.0 and set your desired workers, recommend G.2X and 200 workers for the full planet.
"""

from awsglue.context import GlueContext
from pyspark.context import SparkContext
from sedona.spark import SedonaContext
from transportation_splitter import (
    InputFormat,
    OutputFormat,
    OvertureTransportationSplitter,
    SplitConfig,
    SplitterDataWrangler,
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Register Sedona functions and get Sedona-enabled Spark session
sedona_context = SedonaContext.create(spark)

################################################################
### Set these variables for your environment:

OVERTURE_RELEASE_VERSION = "2025-12-17.0"
MY_S3_BUCKET_OUTPUT_PATH = f"s3://meta-overture-staging/transportation_splitter/{OVERTURE_RELEASE_VERSION}/colorado"

# Colorado is a nice, rectangular State with long roads and a few dense cities, great for testing.
# This test should take about 6 minutes with G.2X and 50 workers with full debug output.
COLORADO = "POLYGON((-109.0602 41.0034, -102.0416 41.0034, -102.0416 36.9925, -109.0602 36.9925, -109.0602 41.0034))"

################################################################

wrangler = SplitterDataWrangler(
    input_path=f"s3://overturemaps-us-west-2/release/{OVERTURE_RELEASE_VERSION}/theme=transportation/",
    output_path=MY_S3_BUCKET_OUTPUT_PATH,
    input_format=InputFormat.PARQUET_WKB,
    output_format=OutputFormat.GEOPARQUET,
    write_intermediate_files=True,
    filter_wkt=COLORADO,
)

config = SplitConfig(
    split_at_connectors=False,
    skip_debug_output=False,
)

splitter = OvertureTransportationSplitter(
    spark=sedona_context,
    wrangler=wrangler,
    cfg=config,
)

result_df = splitter.split()
