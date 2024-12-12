# Transportation Splitter

Pyspark app for splitting [Overture](https://github.com/OvertureMaps/data) transportation segments to simplify consuming the data.

Note: This repository and project are experimental. Things are likely change until a stable release, but we will keep the documentation here up-to-date.

## Description

Working with multiple connectors and linearly referenced properties can be difficult. The purpose of this notebook is to offer one option in how Overture transportation features can be consumed, by first splitting them into simpler sub-segments.

There are multiple ways this can be done, current behavior is to output for each input segment all its corresponding sub-segments "split" at all its connectors and all `between` length-relative location references (LRs).

The result is a transportation data set where segments have exactly two connectors, one for each end, and no linear references.

New "artificial" connector features are added for all LRs that don't already have connectors.

Result is using same Overture schema as the input, except for segments two columns are added: `start_lr` and `end_lr`.

For `sequence` in property `prohibited_transitions` it also adds fields `start_lr` and `end_lr` to identify to which of the splits the `segment_id` refers to.

For details on the process see [here](/ProcessDetails.md).

If you also have access to other open or proprietary data feeds that map Overture segment ids to other properties, with `between` LR fields or not, these can be consolidated into a single parquet via trivial join by `id`, then processed one time by this splitter to produce easy to consume split segments.

## Getting Started

Any Spark environment should work, but for reference this is tested on:
1. Databricks on Azure (Runtime: 15.4 LTS - Apache Spark 3.5.0, Scala 2.12, Python 3.11)
2. AWS Glue (Version: Glue 4.0 - Spark 3.3.0, Scala 2.12, Python 3.10)

### Dependencies

See [pyproject.toml](/pyproject.toml) for used pip packages or install with `poetry install`.

### Installing

**Local install**
```
poetry install
```

**AWS Glue Notebook example config**
```
%idle_timeout 60
%worker_type G.2X
# Will need more workers to process planet, recommended 100
%number_of_workers 5
%glue_version 4.0

%additional_python_modules apache-sedona==1.7.0,shapely,pyproj
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.3_2.12/1.7.0/sedona-spark-shaded-3.3_2.12-1.7.0.jar,https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.7.0-28.5/geotools-wrapper-1.7.0-28.5.jar
```

**Validating Spark setup**
To ensure environment is properly configured for Sedona, run this test code before attempting to use the notebook:

```
from sedona.spark import *

sedona = SedonaContext.create(spark)
sedona.sql("SELECT ST_POINT(1., 2.) as geom").show()
```

### Executing program

For simplicity all the code and parameters are currently included in one [python script](transportation_splitter.py), please set the input variables with appropriate values for `overture_release_version`, `base_output_path` and optionally `wkt_filter` with a polygon WKT if you want to only process the subset of the Overture data that intersects with it.

The list of columns that are considered for identifying the LRs values to split at is constructed at runtime out of input parquet's schema columns that have a `between` field anywhere in their structure.
If you want to customize that behavior please set columns to include or exclude in `cfg` parameter `SplitConfig` for details.

By default the notebook executes the splitting using settings from `DEFAULT_CFG`, which split at all connectors and LR values from all columns in the input, like this:
```python
split_transportation(spark, sc, input_path, output_path)
```


This for example first filter data within the given polygon then would split only at the LR values from column `road_flags`, and will not split at connectors:
```python
split_transportation(spark, sc, input_path, output_path, wkt_filter="POLYGON(...)", SplitConfig(split_at_connectors=False, lr_columns_to_include=["road_flags"]))
```

If you are using databricks you can also add this repo as a git folder, see instructions [here](https://docs.databricks.com/en/repos/repos-setup.html).

## Version History

* 0.1
    * Initial Release
