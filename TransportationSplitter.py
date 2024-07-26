# Databricks notebook source
# MAGIC %md
# MAGIC Please see instructions and details [here](https://github.com/OvertureMaps/transportation-splitter/blob/main/README.md).
# MAGIC
# MAGIC # AWS Glue notebook - see instructions for magic commands

# COMMAND ----------

from lib import *

# COMMAND ----------

from split_tests import *
# Run the tests before starting splitter
unittest.main(argv=[''], verbosity=2, exit=False)

# COMMAND ----------

overture_release_version = "2024-07-22.0"
overture_release_path = "wasbs://release@overturemapswestus2.blob.core.windows.net" #  "s3://overturemaps-us-west-2/release"
base_output_path = "wasbs://test@ovtpipelinedev.blob.core.windows.net/transportation-splits" # "s3://<bucket>/transportation-split"

wkt_filter = None

# South America polygon
wkt_filter = "POLYGON ((-180 -90, 180 -90, 180 -59, -25.8 -59, -25.8 28.459033, -79.20293 28.05483, -79.947494 24.627045, -86.352539 22.796439, -81.650495 15.845105, -82.60631 10.260871, -84.51781 8.331083, -107.538877 10.879395, -120 -59, -180 -59, -180 -90))"

# Tiny test polygon in Bellevue WA
#wkt_filter = "POLYGON ((-122.1896338 47.6185118, -122.1895695 47.6124029, -122.1792197 47.6129526, -122.1793771 47.6178368, -122.1896338 47.6185118))"

input_path = f"{overture_release_path}/{overture_release_version}/theme=transportation"
filter_target = "global" if not wkt_filter else "filtered"
output_path = f"{base_output_path}/{overture_release_version}/{filter_target}"

result_df = split_transportation(spark, input_path, output_path, wkt_filter, reuse_existing_intermediate_outputs=True)
if "DATABRICKS_RUNTIME_VERSION" in os.environ:
    display(result_df.filter('type == "segment"').limit(50))
else:
    result_df.filter('type == "segment"').show(20, False)
