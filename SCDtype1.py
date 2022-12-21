from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *


def SCDtypeI(spark,sourceObject, targetTableName, listOfMatchColumns=["recid"], listOfColumnsToSkip=[], stream=False,
             checkpointLoc=None):
    ## BATCH-FUNCTION

    def SCDbatch(sourceDF=sourceObject):

        # Declare target-table and column to use for matching
        targetTable = DeltaTable.forName(spark, f"{targetTableName}")
        targetTableColumns = targetTable.toDF().schema.names
        matchCriteria = " AND ".join([f"target.{col} = source.{col}" for col in listOfMatchColumns])
        ColumnsToUpdate = [col for col in targetTableColumns if col not in listOfMatchColumns + listOfColumnsToSkip]

        # Merge source-table into target-table
        targetTable.alias("target").merge(
            sourceDF.alias("source"),
            matchCriteria
        ).whenMatchedUpdate(
            set={'target.' + col: 'source.' + col for col in ColumnsToUpdate}
        ).whenNotMatchedInsertAll().execute()

    ## STREAM-FUNCTION

    def SCDstream(microdf, batchid):
        SCDbatch(sourceDF=microdf)

    if stream == False:
        SCDbatch()
    elif stream == True:
        streamQuery = (sourceObject.writeStream
                       .format("delta")
                       ##.outputMode("append")
                       .foreachBatch(SCDstream)
                       .option("checkpointLocation", checkpointLoc)
                       .option("mergeSchema", True)
                       ##.trigger(once=True)
                       .start()
                       )
