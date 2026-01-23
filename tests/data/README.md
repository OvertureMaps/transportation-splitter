## Transportation Splitter Test Data

#### To update the latest test data, run `duckdb < README.md` in the `tests/data` directory

##### test_e2e.py uses data from 2024 in this bbox

```sql
/*

bbox: `POLYGON ((-122.1896338 47.6185118, -122.1895695 47.6124029, -122.1792197 47.6129526, -122.1793771 47.6178368, -122.1896338 47.6185118))`

COPY (
    SELECT *
    FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=transportation/type=connector/*.parquet')
    WHERE
        bbox.xmin >= -122.1896338 and
        bbox.ymin >= 47.6124029 and
        bbox.xmax <= -122.1792197 and
        bbox.ymax <= 47.6178368
) TO 'connector.parquet';

COPY (
    SELECT *
    FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=transportation/type=segment/*.parquet')
    WHERE
        bbox.xmin >= -122.1896338 and
        bbox.ymin >= 47.6124029 and
        bbox.xmax <= -122.1792197 and
        bbox.ymax <= 47.6178368
) TO 'segment.parquet';
```

```sql
*/
-- DuckDB Queries to fetch the fresh data from Overture S3:
INSTALL spatial;
LOAD spatial;

-- Get the latest Overture release from STAC
SET VARIABLE overture_latest_release = (SELECT latest FROM 'https://stac.overturemaps.org/catalog.json');

SET enable_progress_bar = true;

.timer ON
SELECT getvariable('overture_latest_release') AS latest_overture_release;

.echo ON

COPY(
SELECT
    *
FROM
    read_parquet('s3://overturemaps-us-west-2/release/' || getvariable('overture_latest_release') || '/theme=transportation/type=*/*.parquet', union_by_name=1, hive_partitioning=1)
WHERE
    bbox.xmin >= -105.299518 AND
    bbox.ymin >= 39.9945 AND
    bbox.xmax <= -105.236814 AND
    bbox.ymax <= 40.029985
) TO 'boulder.parquet';
```
