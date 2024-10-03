Queries to fetch updated data

Connectors:
```
COPY (
    SELECT *
    FROM read_parquet('s3://overturemaps-us-west-2/release/2024-09-18.0/theme=transportation/type=connector/*.parquet')
    WHERE
        bbox.xmin >= -122.1896338 and
        bbox.ymin >= 47.6124029 and
        bbox.xmax <= -122.1792197 and
        bbox.ymax <= 47.6178368
) TO 'connector.parquet';
```

Segments:
```
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
