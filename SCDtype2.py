from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *


def SCDIIdelta(spark, sourceObject, targetTableName, matchColumnName="recid", ListOfSCDcolumns=['name', 'segment'], ListOfColumnsToSkip=[], TargetTableAlteration=False, stream=False):
    # Check if targetTable-schema needs updating to accommodate SCD
    if TargetTableAlteration == True:
        # Look up targetTable-columns from schema
        targetTableColumns = DeltaTable.forName(spark, f"{targetTableName}").toDF().schema.names
        # Modify targetTable if not compatible with SCD-columns:
        if 'SCDcurrent' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDcurrent STRING")
        if 'SCDeffectiveDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDeffectiveDate STRING")
        if 'SCDendDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDendDate STRING")
        if 'SCDchangeKey' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDchangeKey STRING")

    ## BATCH-FUNCTION

    def SCDbatch(sourceDF=sourceObject):

        # Declare target-table and column to use for matching
        targetTable = DeltaTable.forName(spark, f"{targetTableName}")
        targetTableColumns = targetTable.toDF().schema.names
        matchColumn = matchColumnName

        # SCD-columns and none-SCD-columns are declared (changes to SCD-columns will trigger insertion of new version-records while none-scd will only trigger update of existing records)
        SCDcolumns = ListOfSCDcolumns
        SCDcolumnsUnwrapped = ", ".join([col for col in SCDcolumns])
        NoneSCDcolumns = [col for col in targetTableColumns if
                          col not in SCDcolumns + [f"{matchColumn}", "SCDcurrent", "SCDeffectiveDate", "SCDendDate",
                                                   "SCDchangeKey"] + ListOfColumnsToSkip]

        updatesDF = (sourceDF
                     .withColumn("SCDcurrent", lit("true"))
                     .withColumn("SCDeffectiveDate", current_timestamp())
                     .withColumn("SCDendDate", lit("NULL"))
                     .withColumn("SCDchangeKey", expr(f"SHA2(concat_ws(',',{SCDcolumnsUnwrapped}),512)"))
                     )

        # Rows with new values in SCD-columns for existing records in the target is set aside
        newLabelsToInsert = (updatesDF
                             .alias("updates")
                             .join(targetTable.toDF().alias("target"), f"{matchColumn}")
                             .where("target.SCDcurrent = true AND updates.SCDchangeKey <> target.SCDchangeKey")
                             )

        # Stage the update by unioning two sets of rows
        # 1. The rows that has just been set aside in above step (these are to be inserted as "current" versions of existing records)
        # 2. Rows that will either update the current attribute-labels of existing records or insert the new labels of new records
        stagedUpdates = (
            newLabelsToInsert
                .selectExpr("NULL as mergeKey", "updates.*")  # Rows for 1
                .union(updatesDF.selectExpr(f"{matchColumn} as mergeKey", "*"))  # Rows for 2.
        )

        # Apply SCD Type 2 operation using merge.
        # None-matching records can be grouped in following two categories: 1) rows reflecting new SCD-values for existing records, and 2) entirely new records.
        # M atching records can be grouped in two categories: 1) existing records with old SCD-values needing to be marked as obsolete and provided and SCDendDate, and 2) existing records where there might/might not be updates to none-SCD-columns
        targetTable.alias("target").merge(
            stagedUpdates.alias("staged_updates"), f"target.{matchColumn} = mergeKey"
        ).whenMatchedUpdate(
            condition="target.SCDcurrent = true AND target.SCDchangeKey <> staged_updates.SCDchangeKey",
            set={  # Set current to false and endDate to source's effective date.
                "SCDcurrent": "false",
                "SCDendDate": current_timestamp()
            }
        ).whenMatchedUpdate(
            condition="target.SCDcurrent = true AND target.SCDchangeKey = staged_updates.SCDchangeKey",
            set={'target.' + col: 'staged_updates.' + col for col in NoneSCDcolumns}
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
                       .option("checkpointLocation",
                               "abfss://jf-container@daxdatalakestorage.dfs.core.windows.net/SCDtypeIIStreamTest")
                       .option("mergeSchema", True)
                       ##.trigger(once=True)
                       .start()
                       )