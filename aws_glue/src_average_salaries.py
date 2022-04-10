import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs


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

# Script generated for node Fielding
Fielding_node1649285325724 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_fielding",
    transformation_ctx="Fielding_node1649285325724",
)

# Script generated for node Salary
Salary_node1649285344912 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_salaries",
    transformation_ctx="Salary_node1649285344912",
)

# Script generated for node Pitching
Pitching_node1649297627670 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_pitching",
    transformation_ctx="Pitching_node1649297627670",
)

# Script generated for node Fielding Apply Mapping
FieldingApplyMapping_node1649348061178 = ApplyMapping.apply(
    frame=Fielding_node1649285325724,
    mappings=[("playerid", "string", "fielding_playerid", "string")],
    transformation_ctx="FieldingApplyMapping_node1649348061178",
)

# Script generated for node Salary Apply Mapping
SalaryApplyMapping_node1649348080253 = ApplyMapping.apply(
    frame=Salary_node1649285344912,
    mappings=[
        ("salary", "int", "salary_salary", "int"),
        ("yearid", "int", "salary_yearid", "int"),
        ("playerid", "string", "salary_playerid", "string"),
    ],
    transformation_ctx="SalaryApplyMapping_node1649348080253",
)

# Script generated for node Pitching Apply Mapping
PitchingApplyMapping_node1649348096946 = ApplyMapping.apply(
    frame=Pitching_node1649297627670,
    mappings=[("playerid", "string", "pitching_playerid", "string")],
    transformation_ctx="PitchingApplyMapping_node1649348096946",
)

# Script generated for node Fielding Salary Join
FieldingSalaryJoin_node1649348457332 = Join.apply(
    frame1=FieldingApplyMapping_node1649348061178,
    frame2=SalaryApplyMapping_node1649348080253,
    keys1=["fielding_playerid"],
    keys2=["salary_playerid"],
    transformation_ctx="FieldingSalaryJoin_node1649348457332",
)

# Script generated for node Pitching Salary Join
PitchingSalaryJoin_node1649348424453 = Join.apply(
    frame1=PitchingApplyMapping_node1649348096946,
    frame2=SalaryApplyMapping_node1649348080253,
    keys1=["pitching_playerid"],
    keys2=["salary_playerid"],
    transformation_ctx="PitchingSalaryJoin_node1649348424453",
)

# Script generated for node Fielding Salary Aggregate
FieldingSalaryAggregate_node1649348674254 = sparkAggregate(
    glueContext,
    parentFrame=FieldingSalaryJoin_node1649348457332,
    groups=["salary_yearid"],
    aggs=[["salary_salary", "avg"]],
    transformation_ctx="FieldingSalaryAggregate_node1649348674254",
)

# Script generated for node Pitching Salary Aggregate
PitchingSalaryAggregate_node1649348682452 = sparkAggregate(
    glueContext,
    parentFrame=PitchingSalaryJoin_node1649348424453,
    groups=["salary_yearid"],
    aggs=[["salary_salary", "avg"]],
    transformation_ctx="PitchingSalaryAggregate_node1649348682452",
)

# Script generated for node Fielding Salary Apply Mapping
FieldingSalaryApplyMapping_node1649348800192 = ApplyMapping.apply(
    frame=FieldingSalaryAggregate_node1649348674254,
    mappings=[
        ("salary_yearid", "int", "fielding_salary_yearid", "int"),
        ("`avg(salary_salary)`", "double", "Fielding", "double"),
    ],
    transformation_ctx="FieldingSalaryApplyMapping_node1649348800192",
)

# Script generated for node Pitching Salary Apply Mapping
PitchingSalaryApplyMapping_node1649348794825 = ApplyMapping.apply(
    frame=PitchingSalaryAggregate_node1649348682452,
    mappings=[
        ("salary_yearid", "int", "pitching_salary_yearid", "int"),
        ("`avg(salary_salary)`", "double", "Pitching", "double"),
    ],
    transformation_ctx="PitchingSalaryApplyMapping_node1649348794825",
)

# Script generated for node Fielding Pitching Salary Join
FieldingPitchingSalaryJoin_node1649348977468 = Join.apply(
    frame1=PitchingSalaryApplyMapping_node1649348794825,
    frame2=FieldingSalaryApplyMapping_node1649348800192,
    keys1=["pitching_salary_yearid"],
    keys2=["fielding_salary_yearid"],
    transformation_ctx="FieldingPitchingSalaryJoin_node1649348977468",
)

# Script generated for node Apply Mapping
ApplyMapping_node1649349022407 = ApplyMapping.apply(
    frame=FieldingPitchingSalaryJoin_node1649348977468,
    mappings=[
        ("pitching_salary_yearid", "int", "Year", "int"),
        ("Pitching", "double", "Pitching", "double"),
        ("Fielding", "double", "Fielding", "double"),
    ],
    transformation_ctx="ApplyMapping_node1649349022407",
)

# Script generated for node Amazon S3
AmazonS3_node1649349066081 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1649349022407,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://nodel-ag/ex_1/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1649349066081",
)

job.commit()
