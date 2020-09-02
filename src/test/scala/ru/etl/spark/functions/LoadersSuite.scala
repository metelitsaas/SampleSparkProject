package ru.etl.spark.functions

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.hive.test.TestHiveSingleton


class LoadersSuite
    extends QueryTest
        with TestHiveSingleton {

    import spark.implicits._

    test("overwriteTable function should write DataFrame to table") {
        val loadTable = "overwriteTableTest"
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("key")

        spark.sql(f"CREATE TABLE $loadTable (key int)")

        // Load table first time
        Loaders.overwriteTable(sourceDf, f"$loadTable")

        // Check
        checkAnswer(
            spark.sql(f"SELECT * FROM $loadTable"),
            sourceDf
        )

        // Load table second time
        Loaders.overwriteTable(sourceDf, f"$loadTable")

        // Check that the result is same
        checkAnswer(
            spark.sql(f"SELECT * FROM $loadTable"),
            sourceDf
        )
    }

    test("insertTable function should write DataFrame to table") {
        val loadTable = "insertTableTest"
        val sourceDf = Seq(1, 2, 3, 4, 5, 5, 5, 5, 5, 5)
            .toDF("key")

        spark.sql(f"CREATE TABLE $loadTable (key int)")

        // Load table first time
        Loaders.insertTable(sourceDf, f"$loadTable")

        // Check
        checkAnswer(
            spark.sql(f"SELECT * FROM $loadTable"),
            sourceDf
        )

        // Load table second time
        Loaders.insertTable(sourceDf, f"$loadTable")

        // Check that the result is same
        checkAnswer(
            spark.sql(f"SELECT * FROM $loadTable"),
            sourceDf.union(sourceDf)
        )

    }

}
