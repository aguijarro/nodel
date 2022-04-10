import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame
from pyspark.sql import functions as SqlFuncs


def sparkUnion(glueContext, unionType, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(
        "(select * from source1) UNION " + unionType + " (select * from source2)"
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


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

# Script generated for node Team Place
TeamPlace_node1649366307670 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_teams",
    transformation_ctx="TeamPlace_node1649366307670",
)

# Script generated for node First Place Team
FirstPlaceTeam_node1649367951129 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_teams",
    transformation_ctx="FirstPlaceTeam_node1649367951129",
)

# Script generated for node Last Place Team
LastPlaceTeam_node1649368002894 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_teams",
    transformation_ctx="LastPlaceTeam_node1649368002894",
)

# Script generated for node Team Place Mapping
TeamPlaceMapping_node1649366404561 = ApplyMapping.apply(
    frame=TeamPlace_node1649366307670,
    mappings=[("rank", "int", "rank", "int"), ("yearid", "int", "yearid", "int")],
    transformation_ctx="TeamPlaceMapping_node1649366404561",
)

# Script generated for node First Place Team Mapping
FirstPlaceTeamMapping_node1649368017888 = ApplyMapping.apply(
    frame=FirstPlaceTeam_node1649367951129,
    mappings=[
        ("teamid", "string", "fpt_teamid", "string"),
        ("ab", "int", "fpt_ab", "int"),
        ("rank", "int", "fpt_rank", "int"),
        ("yearid", "int", "fpt_yearid", "int"),
    ],
    transformation_ctx="FirstPlaceTeamMapping_node1649368017888",
)

# Script generated for node Last Place Team Mapping
LastPlaceTeamMapping_node1649368027682 = ApplyMapping.apply(
    frame=LastPlaceTeam_node1649368002894,
    mappings=[
        ("teamid", "string", "lpt_teamid", "string"),
        ("ab", "int", "lpt_ab", "int"),
        ("rank", "int", "lpt_rank", "int"),
        ("yearid", "int", "lpt_yearid", "int"),
    ],
    transformation_ctx="LastPlaceTeamMapping_node1649368027682",
)

# Script generated for node First Place
FirstPlace_node1649367797598 = sparkAggregate(
    glueContext,
    parentFrame=TeamPlaceMapping_node1649366404561,
    groups=["yearid"],
    aggs=[["rank", "min"]],
    transformation_ctx="FirstPlace_node1649367797598",
)

# Script generated for node Last Place
LastPlace_node1649367805097 = sparkAggregate(
    glueContext,
    parentFrame=TeamPlaceMapping_node1649366404561,
    groups=["yearid"],
    aggs=[["rank", "max"]],
    transformation_ctx="LastPlace_node1649367805097",
)

# Script generated for node First Place Tean Join
FirstPlaceTeanJoin_node1649371181569 = Join.apply(
    frame1=FirstPlace_node1649367797598,
    frame2=FirstPlaceTeamMapping_node1649368017888,
    keys1=["yearid", "`min(rank)`"],
    keys2=["fpt_yearid", "fpt_rank"],
    transformation_ctx="FirstPlaceTeanJoin_node1649371181569",
)

# Script generated for node Last Place Team Join
LastPlaceTeamJoin_node1649371312829 = Join.apply(
    frame1=LastPlace_node1649367805097,
    frame2=LastPlaceTeamMapping_node1649368027682,
    keys1=["yearid", "`max(rank)`"],
    keys2=["lpt_yearid", "lpt_rank"],
    transformation_ctx="LastPlaceTeamJoin_node1649371312829",
)

# Script generated for node FPT Apply Mapping
FPTApplyMapping_node1649371945179 = ApplyMapping.apply(
    frame=FirstPlaceTeanJoin_node1649371181569,
    mappings=[
        ("yearid", "int", "yearid", "int"),
        ("`min(rank)`", "int", "rank", "int"),
        ("fpt_teamid", "string", "teamid", "string"),
        ("fpt_ab", "int", "at_bats", "int"),
    ],
    transformation_ctx="FPTApplyMapping_node1649371945179",
)

# Script generated for node LPT Apply Mapping
LPTApplyMapping_node1649371950627 = ApplyMapping.apply(
    frame=LastPlaceTeamJoin_node1649371312829,
    mappings=[
        ("yearid", "int", "yearid", "int"),
        ("`max(rank)`", "int", "rank", "int"),
        ("lpt_teamid", "string", "teamid", "string"),
        ("lpt_ab", "int", "at_bats", "int"),
    ],
    transformation_ctx="LPTApplyMapping_node1649371950627",
)

# Script generated for node Union
Union_node1649372436225 = sparkUnion(
    glueContext,
    unionType="ALL",
    mapping={
        "source1": FPTApplyMapping_node1649371945179,
        "source2": LPTApplyMapping_node1649371950627,
    },
    transformation_ctx="Union_node1649372436225",
)

# Script generated for node Amazon S3
AmazonS3_node1649372552393 = glueContext.write_dynamic_frame.from_options(
    frame=Union_node1649372436225,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://nodel-ag/ex_4/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1649372552393",
)

job.commit()
