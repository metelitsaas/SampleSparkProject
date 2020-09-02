package ru.etl.spark.functions

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SaveMode


// DataFrame load functions
object Loaders {

    /* Write DataFrame by overwrite
    @param df: Source DataFrame
    @param tableName: Target table name */
    def overwriteTable(df: DataFrame, tableName: String): Unit = {
        df
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .insertInto(f"$tableName")
    }

    /* Write DataFrame by insert
    @param df: Source DataFrame
    @param tableName: Target table name */
    def insertTable(df: DataFrame, tableName: String): Unit = {
        df
            .write
            .mode(SaveMode.Append)
            .format("parquet")
            .insertInto(f"$tableName")
    }

}
