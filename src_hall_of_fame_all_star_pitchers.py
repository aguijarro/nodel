import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
import re


def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Pitching
Pitching_node1649333718925 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_pitching",
    transformation_ctx="Pitching_node1649333718925",
)

# Script generated for node Appearance
Appearance_node1649345390747 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_appearances",
    transformation_ctx="Appearance_node1649345390747",
)

# Script generated for node Hall of Fame
HallofFame_node1649346205800 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_halloffame",
    transformation_ctx="HallofFame_node1649346205800",
)

# Script generated for node Pitching Apply Mapping
PitchingApplyMapping_node1649344891294 = ApplyMapping.apply(
    frame=Pitching_node1649333718925,
    mappings=[
        ("era", "double", "pitching_era", "double"),
        ("playerid", "string", "pitching_playerid", "string"),
    ],
    transformation_ctx="PitchingApplyMapping_node1649344891294",
)

# Script generated for node Appearance Apply Mapping
AppearanceApplyMapping_node1649345419286 = ApplyMapping.apply(
    frame=Appearance_node1649345390747,
    mappings=[("playerid", "string", "appearance_playerid", "string")],
    transformation_ctx="AppearanceApplyMapping_node1649345419286",
)

# Script generated for node Hall of Fame Apply Mapping
HallofFameApplyMapping_node1649346237723 = ApplyMapping.apply(
    frame=HallofFame_node1649346205800,
    mappings=[
        ("inducted", "string", "hall_of_fame_inducted", "string"),
        ("playerid", "string", "hall_of_fame_playerid", "string"),
        ("yearid", "int", "hall_of_fame_yearid", "int"),
    ],
    transformation_ctx="HallofFameApplyMapping_node1649346237723",
)

# Script generated for node Pitching Aggregate
PitchingAggregate_node1649345641441 = sparkAggregate(
    glueContext,
    parentFrame=PitchingApplyMapping_node1649344891294,
    groups=["pitching_playerid"],
    aggs=[["pitching_era", "avg"]],
    transformation_ctx="PitchingAggregate_node1649345641441",
)

# Script generated for node Appearance Aggregate
AppearanceAggregate_node1649345522941 = sparkAggregate(
    glueContext,
    parentFrame=AppearanceApplyMapping_node1649345419286,
    groups=["appearance_playerid"],
    aggs=[["appearance_playerid", "count"]],
    transformation_ctx="AppearanceAggregate_node1649345522941",
)

# Script generated for node Hall of Fame Filter
HallofFameFilter_node1649346388219 = Filter.apply(
    frame=HallofFameApplyMapping_node1649346237723,
    f=lambda row: (bool(re.match("Y", row["hall_of_fame_inducted"]))),
    transformation_ctx="HallofFameFilter_node1649346388219",
)

# Script generated for node Pitching Appearance Join
PitchingAppearanceJoin_node1649345815784 = Join.apply(
    frame1=PitchingAggregate_node1649345641441,
    frame2=AppearanceAggregate_node1649345522941,
    keys1=["pitching_playerid"],
    keys2=["appearance_playerid"],
    transformation_ctx="PitchingAppearanceJoin_node1649345815784",
)

# Script generated for node Pitching Appeareance Hall of Fame Join
PitchingAppeareanceHallofFameJoin_node1649346491288 = Join.apply(
    frame1=HallofFameFilter_node1649346388219,
    frame2=PitchingAppearanceJoin_node1649345815784,
    keys1=["hall_of_fame_playerid"],
    keys2=["pitching_playerid"],
    transformation_ctx="PitchingAppeareanceHallofFameJoin_node1649346491288",
)

# Script generated for node Pitching Appeareance Hall of Fame Apply Mapping
PitchingAppeareanceHallofFameApplyMapping_node1649346572012 = ApplyMapping.apply(
    frame=PitchingAppeareanceHallofFameJoin_node1649346491288,
    mappings=[
        ("hall_of_fame_playerid", "string", "Player", "string"),
        ("hall_of_fame_yearid", "int", "Hall_Fame_Induction_Year", "int"),
        ("`avg(pitching_era)`", "double", "ERA", "double"),
        ("`count(appearance_playerid)`", "long", "All_Star_Appearances", "long"),
    ],
    transformation_ctx="PitchingAppeareanceHallofFameApplyMapping_node1649346572012",
)

# Script generated for node Amazon S3
AmazonS3_node1649337000400 = glueContext.write_dynamic_frame.from_options(
    frame=PitchingAppeareanceHallofFameApplyMapping_node1649346572012,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://nodel-ag/ex_2/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1649337000400",
)

job.commit()
