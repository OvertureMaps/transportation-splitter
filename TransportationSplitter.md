# Transportation Segment Splitter
Description and instructions for running [TransportationSplitter notebook](TransportationSplitter.py).

## Purpose

Construct a transportation data set where segments have exactly two connectors, one for each end, and no linear references.

New connector features are added for all LRs that don't already have connectors.

Result is using same overture schema as the input, except for segments two columns are added: start_lr and end_lr.

All resulting segments have exactly two connectors - one at each end of their geometry.

This is deliberately not taking dependencies of any other overture libraries to be easier to integrate and execute outside that code base.

### Tested config
This was tested on `2024-01-17-alpha.0` Overture release as input, with a filter polygon covering South America, total runtime ~10min with the following cluster config on Databricks:

10-48 Workers: 640-3,072 GB Memory, 80-384 Cores

1 Driver: 128 GB Memory, 16 Cores

Runtime: 13.3.x-scala2.12


**Spark config** (needed for sedona)
```
spark.serializer org.apache.spark.serializer.KryoSerializer
spark.kryo.registrator org.apache.sedona.core.serde.SedonaKryoRegistrator
spark.sql.extensions org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions
```

### Dependencies
pip install apache-sedona shapely

Following packages versions were used:
```
apache-sedona = "1.5.1"
shapely = "2.0.2"
```

### AWS Glue Notebook example config
```
%idle_timeout 60
%worker_type G.2X
%number_of_workers 50

%additional_python_modules apache-sedona==1.5.1,shapely
%extra_jars https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.0_2.12/1.5.1/sedona-spark-shaded-3.0_2.12-1.5.1.jar,https://repo1.maven.org/maven2/org/datasyslab/geotools-wrapper/1.5.1-28.2/geotools-wrapper-1.5.1-28.2.jar
%%configure
{
  "--conf": "spark.serializer=org.apache.spark.serializer.KryoSerializer --conf spark.kryo.registrator=org.apache.sedona.core.serde.SedonaKryoRegistrator --conf spark.sql.extensions=org.apache.sedona.viz.sql.SedonaVizExtensions,org.apache.sedona.sql.SedonaSqlExtensions"
}
```

## Algo Steps
### 1. Spatial Filter (optional)
If `filter_wkt` is provided, then it will start will filtering the input to only keep features that intersect with that polygon. There will be some segments that cross that polygon, so some of their connectors may not get included in the output. That is fine, the rest of the algo will create "artificial" connectors for start and end of segments whenever they are not found as actual connector features.
### 2. Join connectors into segments
Explode segments by connector properties to get segment id -> connector id pairs, then join with connectors on connector id to get connector geometry.
Group by segment id again and construct `joined_connectors` column with all connector geometries.

### 3. Split segments UDF
- Initialize split points from joined connectors
- Find all LRs from the properties (road and names)
- Add split points for all LRs
  - try to snap to one of the existing split points within some tolerance
  - introduce new split point if needed
- Order split points by their LR
- Create segment splits for every pair of subsequent split points
  - Calculate sub-segment geometry - iterate over original geometry coordinates and keep those within the split points LRs
  - Clone all properties from original segment
  - Apply sub-segment scoping - for all properties with LR scope remove the scope or the whole property if the LR is outside the current sub-segment
- Create new connector features for each added split point from LR scoped properties

Result from UDF at this step is saved as intermediate output for debug purposes with columns for each input segment row:
- input segment row
- split segments
- added connectors
- exception
- elapsed runtime

### 4. Reformat
From the result from UDF at step 3 the split segments and added connectors are exploded into individual rows.

Schema is identical with input schema from Overture transportation data set with one exception - two new columns are introduced since original segment id is no longer sufficient to uniquely identify each segment feature:
- `start_lr`
- `end_lr`


### 5. Turn restriction resolution
After split, `road.restrictions.prohibited_transitions` property has to be fixed:
1. Any original turn restriction applies to exactly one split, we need to identify to which and remove it from the other splits.
2. All turn restrictions sequences have a reference to a `segment_id`. We need to identify which split for that segment id and fully qualify the reference from the turn restriction.

Part 1 is partially resolved in the UDF at step 3 because we can do an early prunning of the references we need to fix for part 2 - all splits that don't start or end with the connerctor at first position in the turn restriction's `sequence` are dropped.
Part 2 is addressed by first joining the turn restriction reference with the segment corpus by segment id to get the all the splits and their corresponding two connectors ids. The will be at most two splits that share the referenced connector. To identify which of the two splits:
- if last reference in the sequence, `final_heading` is used to identify at which position in the `connector_ids` for the split segment do we expect the referenced `connector_id`.
- for all other, the split segment's `connector_ids` must consist of the `connector_id` from the turn restriction at that position in the sequence and the `connector_id` from the sequence in the very next position.
So we can work back from the last segment to pick which splits to retain for each step, and augment the turn restriction's `segment_id` references with the `start_lr` and `end_lr` of the correct split.

One simple approach in consuming this split corpus would be to append the split lrs to each segment id, for example `id = f"{id}@{start_lr}-{end_lr}"`, do the same for the turn restriction's `segment_id` references, and discard the start_lr, end_lr columns and turn restriction properties after that.

This has the benefit of having a single column globally unique identifier (as opposed to composite {id,start_lr,end_lr}) for the split segments and data compliant with the Overture schema.
