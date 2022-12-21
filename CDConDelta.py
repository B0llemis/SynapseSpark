from delta import *
from pyspark.sql.functions import *
from pyspark.sql.column import *


def CDCdelta(spark,sourceTableName: str, CDCcounterLoc: str, updateCounter=True) -> tuple:
    deltaTable = DeltaTable.forName(spark, f"{sourceTableName}")
    maxVersion = deltaTable.history(1)[["version"]]
    maxVersionInt = maxVersion.head()[0]
    if maxVersionInt == 0:
        dfChanges = deltaTable.toDF()
    else:
        try:
            lastRunVersionInt = spark.read.format("json").load(CDCcounterLoc).head()[0]
        except:
            dfChanges = deltaTable.toDF()
        else:
            dfNew = spark.read.option("versionAsOf", maxVersionInt).table(f"{sourceTableName}")
            dfOld = spark.read.option("versionAsOf", lastRunVersionInt).table(f"{sourceTableName}")
            dfChanges = dfNew.subtract(dfNew.intersect(dfOld))

    if updateCounter == True:
        maxVersion.write.mode("overwrite").format("json").save(CDCcounterLoc)

    return (dfChanges, maxVersion)
