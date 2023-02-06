from pyspark.sql.functions import *
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.window import Window


def MergeNinja(
        sourceDataFrame
        , targetDeltaTable
        , columnsToMatch: list
        , columnsToSkip: list
        , typeIIColumns=[]
        , partitionPruningColumn: str = None
        , surrogateKeyColumn: str = None
        , stream: bool = False
        , checkpointSubFolder: str = None
        , softDelete: bool = False
        , targetTableAlteration: bool = False
):
    ### Check if targetTable-schema needs updating to accommodate SCD
    if targetTableAlteration == True:
        # Look up targetTable-columns from schema
        targetTableName = targetDeltaTable.detail().first()['name']
        targetTableColumns = DeltaTable.forPath(spark, targetDeltaTable).toDF().schema.names
        # Modify targetTable if not compatible with SCD-columns:
        if 'SCDcurrent' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDcurrent STRING")
        if 'SCDeffectiveDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDeffectiveDate STRING")
        if 'SCDendDate' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDendDate STRING")
        if 'SCDcompareKey' not in targetTableColumns:
            spark.sql(f"ALTER TABLE {targetTableName} ADD COLUMN SCDcompareKey STRING")

    ### BATCH-FUNCTION

    def SCDbatch(sourceDF=sourceDataFrame):

        ## Declare target-table
        targetTable = targetDeltaTable

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
        ignoreColumns = [col for col in columnsToSkip + [surrogateKeyColumn] if col != None]
        matchColumns = columnsToMatch
        compareColumns = [col for col in targetTableColumns if
                          col not in matchColumns + auditColumns + ignoreColumns] if typeIIColumns == '*' else typeIIColumns
        synchColumns = [col for col in targetTableColumns if
                        col not in matchColumns + auditColumns + ignoreColumns + compareColumns]

        ## Defines criterias used in the different Merge-operation scenarios
        # mergeKeyDefinition - hash-concationation of matchColumns into a single field on the source-side
        # matchCriteria - applying the same hash-concatination on matchColumns on the target-side and comparing with source-side mergeKey. Alternatively adding Partition Pruning clause on target-side.
        # newVersionCriteria - checking if compareColumns differ on source-side and target-side. Clause is turned off if no typeIIcolumns are provided.
        # synchCriteria - checking if compareColumns are identical on source-side and target-side. Clause is turned on if no typeIIcolumns are provided.

        mergeKeyDefinition = f"""SHA2(concat_ws(',',{", ".join([col for col in matchColumns])}),512) AS mergeKey"""
        matchCriteria = f"""SHA2(concat_ws(',',{", ".join(['target.' + col for col in matchColumns])}),512) = mergeKey"""
        newVersionCriteria = "false" if typeIIColumns == [] else f"""target.SCDcurrent = true AND ({" OR ".join([f"target.{col} <> source.{col}" for col in compareColumns])})"""
        synchCriteria = "true" if typeIIColumns == [] else f"""target.SCDcurrent = true AND {" AND ".join([f"target.{col} = source.{col}" for col in compareColumns])}"""

        ## Define column-maps used in the different Merge-operation scenarios
        newVersionMap = {
            "target.SCDcurrent": "false",
            "target.SCDendDate": current_timestamp()
        }
        synchMap = {'target.' + col: 'source.' + col for col in synchColumns}

        ## The Audit-columns are added to the source
        updatesDF = (sourceDF
                     .withColumn("SCDcurrent", lit("true"))
                     .withColumn("SCDeffectiveDate", current_timestamp())
                     .withColumn("SCDendDate", lit("NULL"))
                     # .withColumn("SCDcompareKey",expr(f"""SHA2(concat_ws(',',{", ".join([col for col in compareColumns])}),512)"""))
                     )

        ## OPTIONAL: Partition Pruning-clause is added to the match-criteria.
        if partitionPruningColumn != None:
            activePartitions = ", ".join([row[0] for row in sourceDF.select(f"{partitionPruningColumn}").collect()])
            matchCriteria += f" AND target.{partitionPruningColumn} IN ({activePartitions})"

        ## OPTIONAL: SurrogateKey-values are generated based on row-number ordering if feature is turned on
        if surrogateKeyColumn != None:
            maxKey = targetTable.toDF().selectExpr(f"max({surrogateKeyColumn})").first()[0]
            window = Window.orderBy(lit("1"))
            updatesDF = updatesDF.withColumn(surrogateKeyColumn, int(maxKey or 0) + row_number().over(window))

        ## Source-rows with new values in the Compare-columns are identified
        newVersions = (updatesDF.alias("source")
                       .join(
            targetTable.toDF().alias("target"),
            matchColumns
        )
                       .where(newVersionCriteria)
                       )

        ## Stage the update by unioning two sets of rows
        # 1. The rows that has just been set aside in above step (these are to be inserted as "current" versions of existing records)
        # 2. Rows that will either update the current attribute-labels of existing records or insert the new labels of new records
        stackedUpserts = (
            newVersions
            .selectExpr("NULL as mergeKey", "source.*")  # Rows for 1
            .union(updatesDF.selectExpr(mergeKeyDefinition, "*"))  # Rows for 2.
        )

        ## OPTIONAL: Soft-Delete
        if softDelete == True:
            deletedRows = (targetTable.toDF().alias('target')
                           .join(
                updatesDF.alias('source'),
                matchColumns,
                'left_anti'
            )
                           .where('target.SCDcurrent=true')
                           .withColumn('SCDcurrent', lit('deleted'))
                           )
            stackedUpserts = (stackedUpserts
                              .union(deletedRows.selectExpr(mergeKeyDefinition, "*"))
                              )
            synchCriteria += " OR source.SCDcurrent = 'deleted'"
            synchMap['target.SCDcurrent'] = 'source.SCDcurrent'

        ## Execute merge-operation.
        # None-matching records can be grouped in following two categories: 1) rows reflecting new SCD-values for existing records, and 2) entirely new records.
        # Matching records can be grouped in two categories: 1) existing records with old SCD-values needing to be marked as obsolete and provided and SCDendDate, and 2) existing records where there might/might not be updates to none-SCD-columns
        (targetTable.alias("target")
         .merge(
            stackedUpserts.alias("source"),
            matchCriteria
        ).whenMatchedUpdate(
            condition=synchCriteria,
            set=synchMap
        ).whenMatchedUpdate(
            condition=newVersionCriteria,
            set=newVersionMap
        ).whenNotMatchedInsertAll()
         .execute()
         )

    ## STREAM-FUNCTION

    def SCDstream(microdf, batchid):
        SCDbatch(sourceDF=microdf)

    if stream == False:
        SCDbatch()
    elif stream == True:
        targetTableLoc = targetDeltaTable.detail().first()['location']
        streamQuery = (sourceDataFrame.writeStream
                       .format("delta")
                       .foreachBatch(SCDstream)
                       .option("checkpointLocation", targetTableLoc + '/_checkpoint' + (
            f'/{checkpointSubFolder}' if checkpointSubFolder != None else ''))
                       .option("mergeSchema", True)
                       .trigger(once=True)
                       .start()
                       )
