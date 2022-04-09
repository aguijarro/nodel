import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsglue.dynamicframe import DynamicFrame
from awsglue import DynamicFrame

# Script generated for node Pitching Post WinLoss Custom
def MyTransform(glueContext, dfc) -> DynamicFrameCollection:
    from pyspark.sql.window import Window
    from pyspark.sql.functions import col, row_number

    df = dfc.select(list(dfc.keys())[0]).toDF()

    windowYear = Window.partitionBy("yearid").orderBy(col("era").desc())
    df_window_year = df.withColumn("row", row_number().over(windowYear))
    df_filtered = df_window_year.filter(df_window_year["row"] < 11)

    # dyf_window_year = DynamicFrame.fromDF(df_window_year, glueContext, "split_year")
    dyf_window_year = DynamicFrame.fromDF(df_filtered, glueContext, "split_year")
    return DynamicFrameCollection(
        {"Pitching Post WinLoss Custom": dyf_window_year}, glueContext
    )


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Pitching Post
PitchingPost_node1649389668127 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_pitchingpost",
    transformation_ctx="PitchingPost_node1649389668127",
)

# Script generated for node Pitching Regular Season
PitchingRegularSeason_node1649462415683 = glueContext.create_dynamic_frame.from_catalog(
    database="lahman2016",
    table_name="lahman2016_pitching",
    transformation_ctx="PitchingRegularSeason_node1649462415683",
)

# Script generated for node Pitching Post Apply Mapping
PitchingPostApplyMapping_node1649389716934 = ApplyMapping.apply(
    frame=PitchingPost_node1649389668127,
    mappings=[
        ("l", "int", "l", "int"),
        ("teamid", "string", "teamid", "string"),
        ("w", "int", "w", "int"),
        ("yearid", "int", "yearid", "int"),
        ("era", "string", "era", "string"),
        ("playerid", "string", "playerid", "string"),
    ],
    transformation_ctx="PitchingPostApplyMapping_node1649389716934",
)

# Script generated for node Pitching Regular Season Mapping
PitchingRegularSeasonMapping_node1649462660012 = ApplyMapping.apply(
    frame=PitchingRegularSeason_node1649462415683,
    mappings=[
        ("l", "int", "pitching_l", "int"),
        ("teamid", "string", "pitching_teamid", "string"),
        ("w", "int", "pitching_w", "int"),
        ("yearid", "int", "pitching_yearid", "int"),
        ("era", "double", "pitching_era", "double"),
        ("playerid", "string", "pitching_playerid", "string"),
    ],
    transformation_ctx="PitchingRegularSeasonMapping_node1649462660012",
)

# Script generated for node Pitching Post WinLoss SQL
SqlQuery0 = """
select yearid, playerid, teamid, era, w, l, (w/(w+l)) win_loss from myDataSource

"""
PitchingPostWinLossSQL_node1649391006251 = sparkSqlQuery(
    glueContext,
    query=SqlQuery0,
    mapping={"myDataSource": PitchingPostApplyMapping_node1649389716934},
    transformation_ctx="PitchingPostWinLossSQL_node1649391006251",
)

# Script generated for node Pitching Regular Season SQL
SqlQuery1 = """
select pitching_yearid, pitching_playerid, pitching_teamid, pitching_era, pitching_w, pitching_l, (pitching_w/(pitching_w+pitching_l)) pitching_win_loss from myDataSource

"""
PitchingRegularSeasonSQL_node1649463805159 = sparkSqlQuery(
    glueContext,
    query=SqlQuery1,
    mapping={"myDataSource": PitchingRegularSeasonMapping_node1649462660012},
    transformation_ctx="PitchingRegularSeasonSQL_node1649463805159",
)

# Script generated for node Pitching Post WinLoss Custom
PitchingPostWinLossCustom_node1649443189143 = MyTransform(
    glueContext,
    DynamicFrameCollection(
        {
            "PitchingPostWinLossSQL_node1649391006251": PitchingPostWinLossSQL_node1649391006251
        },
        glueContext,
    ),
)

# Script generated for node Select From Collection
SelectFromCollection_node1649444675760 = SelectFromCollection.apply(
    dfc=PitchingPostWinLossCustom_node1649443189143,
    key=list(PitchingPostWinLossCustom_node1649443189143.keys())[0],
    transformation_ctx="SelectFromCollection_node1649444675760",
)

# Script generated for node Join
Join_node1649463152476 = Join.apply(
    frame1=SelectFromCollection_node1649444675760,
    frame2=PitchingRegularSeasonSQL_node1649463805159,
    keys1=["yearid", "playerid", "teamid"],
    keys2=["pitching_yearid", "pitching_playerid", "pitching_teamid"],
    transformation_ctx="Join_node1649463152476",
)

# Script generated for node Apply Mapping
ApplyMapping_node1649463425913 = ApplyMapping.apply(
    frame=Join_node1649463152476,
    mappings=[
        ("win_loss", "double", "PostSeasonWinLoss", "double"),
        ("yearid", "int", "Year", "int"),
        ("era", "string", "PostSeasonERA", "string"),
        ("playerid", "string", "Player", "string"),
        ("pitching_win_loss", "double", "RegularSeasonWinLoss", "double"),
        ("pitching_era", "double", "RegularSeasonERA", "double"),
    ],
    transformation_ctx="ApplyMapping_node1649463425913",
)

# Script generated for node Amazon S3
AmazonS3_node1649451107075 = glueContext.write_dynamic_frame.from_options(
    frame=ApplyMapping_node1649463425913,
    connection_type="s3",
    format="json",
    connection_options={"path": "s3://nodel-ag/ex_3/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1649451107075",
)

job.commit()
