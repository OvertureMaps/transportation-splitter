-- Use this to create an Athena table post-split
CREATE EXTERNAL TABLE overture_2024_11_13_0_split (
    `id` string,
    `version` int,
    `names` struct<primary: string, common: map<string, string>, rules: array<struct<variant: string, language: string, value: string, between: array<double>, side: string>>>,
    `subtype` varchar(5),
    `class` string,
    `subclass` string,
    `subclass_rules` array<struct<value: string, between: array<double>>>,
    `connectors` array<struct<connector_id:string,at:double>>,
    `road_surface` array<struct<value: string, between: array<double>>>,
    `road_flags` array<struct<values: array<string>, between: array<double>>>,
    `width_rules` array<struct<value: double, between: array<double>>>,
    `level_rules` array<struct<value: int, between: array<double>>>,
    `access_restrictions` array<
        struct<
        access_type: string,
        `when`: struct<
            during: string,
            heading: string,
            `using`: array<string>,
            recognized: array<string>,
            mode: array<string>,
            vehicle: array<
            struct<
                dimension: string,
                comparison: string,
                value: double,
                unit: string
            >
            >
        >,
        between: array<double>
        >
    >,
    `speed_limits` array<
        struct<
        min_speed: struct<value: int, unit: string>,
        max_speed: struct<value: int, unit: string>,
        is_max_speed_variable: boolean,
        `when`: struct<
            during: string,
            heading: string,
            `using`: array<string>,
            recognized: array<string>,
            mode: array<string>,
            vehicle: array<
            struct<
                dimension: string,
                comparison: string,
                value: double,
                unit: string
            >
            >
        >,
        between: array<double>
        >
    >,
    `prohibited_transitions` array<
        struct<
        sequence: array<struct<connector_id: string, segment_id: string>>,
        final_heading: string,
        `when`: struct<
            heading: string,
            during: string,
            `using`: array<string>,
            recognized: array<string>,
            mode: array<string>,
            vehicle: array<
            struct<
                dimension: string,
                comparison: string,
                value: double,
                unit: string
            >
            >
        >,
        between: array<double>
        >
    >,
    `routes` array<struct<name: string, network: string, ref: string, symbol: string, wikidata: string, between: array<double>>>,
    `destinations` array<
    struct<
        labels: array<
        struct<
            value: string,
            type: string
        >
        >,
        symbols: array<string>,
        from_connector_id: string,
        to_segment_id: string,
        to_connector_id: string,
        `when`: struct<
        heading: string
        >,
        final_heading: string
    >
    >,
    `sources` array<struct<property: string, dataset: string, record_id: string, update_time: string, confidence: double>>,
    `source_tags_rules` array<struct<key:string,value:string,between:array<double>>>,
    `geometry` binary,
    -- These LR columns are the only new fields added by the splitter
    `start_lr` double,
    `end_lr` double,
    `theme` varchar(14),
    `type` string
)
ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
    's3://your-bucket/transportation_splitter/2024-11-13.0/global/';
