import sys

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

config = [
    {
        "input_path": "s3://imdb-data-folder/imdb-input/rating/imdb_rating.tsv",
        "output_path": "s3://imdb-data-folder/imdb-transformed/rating/",
        "catalogTableName": "rating",
        "schema": [
            ("tconst", "string", "tconst", "string"),
            ("averageRating", "string", "averageRating", "float"),
            ("numVotes", "string", "numVotes", "int"),
        ],
    },
    {
        "input_path": "s3://imdb-data-folder/imdb-input/episode/imdb_episode.tsv",
        "output_path": "s3://imdb-data-folder/imdb-transformed/episode/",
        "catalogTableName": "episode",
        "schema": [
            ("tconst", "string", "tconst", "string"),
            ("parenttconst", "string", "parenttconst", "string"),
            ("seasonnumber", "string", "seasonnumber", "int"),
            ("episodenumber", "string", "episodenumber", "int"),
        ],
    },
]

for data in config:
    # Script generated for node Rating - Input
    RatingInput_node1704222349587 = glueContext.create_dynamic_frame.from_options(
        format_options={
            "quoteChar": '"',
            "withHeader": True,
            "separator": "\t",
            "optimizePerformance": False,
        },
        connection_type="s3",
        format="csv",
        connection_options={
            "paths": [data["input_path"]],
            "recurse": True,
        },
        transformation_ctx="RatingInput_node1704222349587",
    )

    # Script generated for node Change Schema
    ChangeSchema_node1704222482089 = ApplyMapping.apply(
        frame=RatingInput_node1704222349587,
        mappings=data["schema"],
        transformation_ctx="ChangeSchema_node1704222482089",
    )

    # Deletes files from the specified Amazon S3 path recursively
    Purge_s3_node = glueContext.purge_s3_path(
        data["output_path"],
        options={"retentionPeriod": 0},
        transformation_ctx="Purge_s3_node",
    )

    # Script generated for node Rating - Output
    RatingOutput_node1704222503263 = glueContext.getSink(
        path=data["output_path"],
        connection_type="s3",
        updateBehavior="LOG",
        partitionKeys=[],
        compression="snappy",
        enableUpdateCatalog=True,
        transformation_ctx="RatingOutput_node1704222503263",
    )
    RatingOutput_node1704222503263.setCatalogInfo(
        catalogDatabase="imdb", catalogTableName=data["catalogTableName"]
    )
    RatingOutput_node1704222503263.setFormat("glueparquet")
    RatingOutput_node1704222503263.writeFrame(ChangeSchema_node1704222482089)
job.commit()
