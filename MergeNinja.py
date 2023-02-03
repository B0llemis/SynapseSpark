from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *


def MergeNinja(
        source
        , target
        , columnsToMatch: list
        , typeIIColumns: list
        , columnsToSkip: list
        , partitionPruningColumn: str = None
        , stream=False
        , checkpointSubFolder: str = None
        , targetTableAlteration=False
):
    ### Check if targetTable-schema needs updating to accommodate SCD
    if targetTableAlteration == True:
        # Look up targetTable-columns from schema
        targetTableColumns = DeltaTable.forPath(spark, target).toDF().schema.names
        # Modify targetTable if not compatible with SCD-columns:
        if 'SCDcurrent' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {target} ADD COLUMN SCDcurrent STRING")
        if 'SCDeffectiveDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {target} ADD COLUMN SCDeffectiveDate STRING")
        if 'SCDendDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {target} ADD COLUMN SCDendDate STRING")
        if 'SCDcompareKey' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {target} ADD COLUMN SCDcompareKey STRING")

    ### BATCH-FUNCTION

    def SCDbatch(sourceDF=source):

        ## Declare target-table
        targetTable = DeltaTable.forPath(spark, target)

        ## During Merge-operation the fields for a given row in the target-tabel can be separated into following categories based on intented behaviour:
        # Total - all the fields in the target-table
        # Audit - representing the system-values used to manage and monitor the SCD-status of each record
        # Ignore - the fields not intended to be impacted by the operation.
        # Match - representing the unique identity-bearing markers defining a given entity-record.
        # Compare - representing the primary attibutes associated with a given entity-record. Changes in these attributes lead to new version-records of the entity.
        # Synchronization - representing the secondary attributes associated with a given entity-records. Changes in these attributes does not warrant creation of new version-records but the values should be kept aligned across source and target.

        targetTableColumns = targetTable.toDF().schema.names
        auditColumns = ['SCDcurrent', 'SCDeffectiveDate',
                        'SCDendDate']  # ['SCDcurrent','SCDeffectiveDate','SCDendDate','SCDcompareKey']
        ignoreColumns = columnsToSkip
        matchColumns = columnsToMatch
        compareColumns = [col for col in targetTableColumns if
                          col not in matchColumns + auditColumns + ignoreColumns] if typeIIColumns == '*' else typeIIColumns
        synchColumns = [col for col in targetTableColumns if
                        col not in matchColumns + auditColumns + ignoreColumns + compareColumns]

        ## Defines criterias used in the different Merge-operation scenarios
        # mergeKeyDefinition - hash-concationation of matchColumns into a single field on the source-side
        # matchCriteria - applying the same has-concation on matchColumns on the target-side and comparing with source-side mergeKey. Alternatively adding Partition Pruning clause on target-side.
        # newVersionCriteria - checking if compareColumns differ on source-side and target-side
        # synchCriteria - checking if compareColumns are identical on source-side and target-side

        mergeKeyDefinition = f"""SHA2(concat_ws(',',{", ".join([col for col in matchColumns])}),512) AS mergeKey"""
        matchCriteria = f"""SHA2(concat_ws(',',{", ".join(['target.' + col for col in matchColumns])}),512) = mergeKey"""
        if partitionPruningColumn != None:
            activePartitions = ", ".join([row[0] for row in sourceDF.select(f"{partitionPruningColumn}").collect()])
            matchCriteria += f" AND target.{partitionPruningColumn} IN ({activePartitions})"
        newVersionCriteria = f"""target.SCDcurrent = true AND ({" OR ".join([f"target.{col} <> source.{col}" for col in compareColumns])})"""
        synchCriteria = "target.SCDcurrent = true AND " + " AND ".join(
            [f"target.{col} = source.{col}" for col in compareColumns])

        ## The Audit-columns are added to the source
        updatesDF = (sourceDF
                     .withColumn("SCDcurrent", lit("true"))
                     .withColumn("SCDeffectiveDate", current_timestamp())
                     .withColumn("SCDendDate", lit("NULL"))
                     # .withColumn("SCDcompareKey",expr(f"""SHA2(concat_ws(',',{", ".join([col for col in compareColumns])}),512)"""))
                     )

        ## Source-rows with new values in the Compare-columns are identified
        newVersions = (updatesDF.alias("source")
                       .join(
            targetTable.toDF().alias("target"),
            matchColumns
        ).where(newVersionCriteria)
                       )

        # Stage the update by unioning two sets of rows
        # 1. The rows that has just been set aside in above step (these are to be inserted as "current" versions of existing records)
        # 2. Rows that will either update the current attribute-labels of existing records or insert the new labels of new records
        stackedUpdates = (
            newVersions
            .selectExpr("NULL as mergeKey", "source.*")  # Rows for 1
            .union(updatesDF.selectExpr(mergeKeyDefinition, "*"))  # Rows for 2.
        )

        # Apply SCD Type 2 operation using merge.
        # None-matching records can be grouped in following two categories: 1) rows reflecting new SCD-values for existing records, and 2) entirely new records.
        # M atching records can be grouped in two categories: 1) existing records with old SCD-values needing to be marked as obsolete and provided and SCDendDate, and 2) existing records where there might/might not be updates to none-SCD-columns
        (targetTable.alias("target")
         .merge(
            stackedUpdates.alias("source"),
            matchCriteria
        ).whenMatchedUpdate(
            condition=newVersionCriteria,
            set={  # Set current to false and endDate to source's effective date.
                "target.SCDcurrent": "false",
                "target.SCDendDate": current_timestamp()
            }
        ).whenMatchedUpdate(
            condition=synchCriteria,
            set={'target.' + col: 'source.' + col for col in synchColumns}
        ).whenNotMatchedInsertAll()
         .execute()
         )

    ## STREAM-FUNCTION

    def SCDstream(microdf, batchid):
        SCDbatch(sourceDF=microdf)

    if stream == False:
        SCDbatch()
    elif stream == True:
        streamQuery = (source.writeStream
                       .format("delta")
                       ##.outputMode("append")
                       .foreachBatch(SCDstream)
                       .option("checkpointLocation", target + '/_checkpoint' + (
            f'/{checkpointSubFolder}' if checkpointSubFolder != None else ''))
                       .option("mergeSchema", True)
                       .trigger(once=True)
                       .start()
                       )
