package ru.etl.spark.functions

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/* Class of general DataFrame transforms
@param sparkSessionConfig: Config of SparkSession */
class Transforms(spark: SparkSession) {

    /* Add processed timestamp columns
    @param df: Source DataFrame
    @return : Target DataFrame */
    def withProcessedDttm(df: DataFrame): DataFrame = {
        df.withColumn("processed_dttm",
            lit(f"${Functions.processedDttm}").cast(TimestampType))
    }

    // TODO: Add DataFrameWriter

    /* Broadcast long tail of DataFrame to resolve data skew
    @param columns: Sequence of Columns with skew
    @param sigma: Sigma value
    @param df: Source DataFrame
    @return : Target DataFrame */
    def broadcastFrequentSkew(columns: Seq[Column], sigma: Integer = 2)
                             (df: DataFrame): DataFrame = {
        // Detecting data skew
        val dataSkew = df
            .groupBy(columns: _*)
            .count
            .agg(stddev("count"), mean("count"))
            .head

        // TODO: Check dataSkew
        val dataStdDev = dataSkew.getDouble(0) // Standard Deviation
        val dataMean = dataSkew.getDouble(1) // AVG

        // Columns window
        val windowByColumn = Window.partitionBy(columns: _*)

        // Values count
        val countDf = df
            .select("*")
            .withColumn("count", count("*").over(windowByColumn))

        val freqDf = countDf // Frequent column values
            .where(col("count") > dataMean + sigma + dataStdDev)

        val infreqDf = countDf // Infrequent column values
            .where(col("count") <= dataMean + sigma + dataStdDev)

        broadcast(freqDf).union(infreqDf).select(df.columns.map(col): _*)
    }

    /* Cast DataFrame timestamp type fields to string
    @param df: Source DataFrame
    @return : Target DataFrame */
    def castTimestampsToStrings(df: DataFrame): DataFrame = {
        df
            .schema
            .fields
            .filter(field => field.dataType match {
                case _ @ (_: DataType | _: TimestampType) => true
                case _ => false }
            ).foldLeft(df){ (newDf, field) => newDf
                .withColumn(field.name, col(field.name).cast(StringType)) }
    }

    /* Calculate partition by object size and HDFS block size
    @param repType: Two types of size calculation:
        "folder" - by folder size in HDFS
        "plan" - by DataFrame's catalyst plan
    @param df: Source DataFrame
    @return : Target DataFrame */
    def repartitionByType(repType: String = "folder")
                         (df: DataFrame): DataFrame = {
        // Choose type of DataFrame size calculation
        val dfSize = repType match {
            case "folder" => sizeOfDataFrameByFolder(df)
            case "plan" => sizeOfDataFrameByPlan(df)
        }

        val hdfsBlockSize = getHdfsBlockSize // HDFS block size from cluster settings
        val count = math.ceil(dfSize.toFloat / hdfsBlockSize).toInt

        df.repartition(count)
    }

    /* HDFS block size in bytes, based on cluster config
    @return : Bytes of HDFS block */
    private def getHdfsBlockSize: Int = {
        spark
            .sparkContext
            .hadoopConfiguration
            .get("dfs.block.size")
            .toInt
    }

    /* Get HDFS folder size by path
    @param folderSize: Path of folder
    @return : Folder size in bytes */
    private def folderSize(path: String): BigInt = {
        val conf = spark.sparkContext.hadoopConfiguration
        val fs = FileSystem.get(conf)
        val hdfsPath = new Path(path)

        fs.getContentSummary(hdfsPath).getLength
    }

    /* Get Hive table location
    @param df: Source DataFrame
    @return : Path of Hive table folder */
    private def hiveTableLocation(df: DataFrame): String = {
        df.createOrReplaceTempView("df")
        spark.sql("desc formatted df")
            .filter("col_name == 'Location'")
            .head(1)(0)
            .getString(0)
    }

    /* Repartition by DataFrame size from optimized plan
    @param df: Source DataFrame
    @return : DataFrame size in bytes */
    private def sizeOfDataFrameByPlan(df: DataFrame): BigInt = {
        val catalystPlan = df.queryExecution.logical // Catalyst logical plan
        val dfSize = spark // Size of DataFrame
            .sessionState
            .executePlan(catalystPlan)
            .optimizedPlan
            .stats
            .sizeInBytes

        dfSize
    }

    /* Calculate partition count by DataFrame folder size
    @param df: Source DataFrame
    @return : DataFrame size in bytes */
    private def sizeOfDataFrameByFolder(df: DataFrame): BigInt = {
        val folder = hiveTableLocation(df)
        val dfSize = folderSize(folder)

        dfSize
    }

}
