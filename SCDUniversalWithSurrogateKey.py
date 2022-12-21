def SCDtypeI(sourceObject, targetTableName, identityColumnName="Key", listOfMatchColumns=["recid"],
             listOfColumnsToSkip=[], stream=False, checkpointLoc=None):
    ## BATCH-FUNCTION

    def SCDbatch(sourceDF=sourceObject):

        # Declare target-table and column to use for matching
        targetTable = DeltaTable.forName(spark, f"{targetTableName}")
        targetTableColumns = targetTable.toDF().schema.names
        matchCriteria = " AND ".join([f"target.{col} = source.{col}" for col in listOfMatchColumns])
        ColumnsToUpdate = [col for col in targetTableColumns if
                           col not in listOfMatchColumns + listOfColumnsToSkip + [identityColumnName]]

        # Last saved surrogate key before performing batch
        maxKey = spark.sql(f"select max({identityColumnName}) from {targetTableName}").first()[
            0]  # First record from single-column, single-row dataset

        # Sleep to mitigate throttle-limit at the DeltaTable-API for the target table
        time.sleep(5.0)

        # Surrogate key sorting column
        window = Window.orderBy(lit("1"))  # Order by dummy value to get regular row number

        # Extend dataframe with audit columns and identity column (surrogate key)
        sourceDF = (sourceDF
                    .withColumn(identityColumnName, maxKey + row_number().over(window))
                    # Column defined in target delta table. Surrogate key column.
                    )

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
                       .trigger(once=True)
                       .start()
                       .awaitTermination()
                       )
