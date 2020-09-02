package ru.etl.spark.utils

import org.scalatest.FunSuite


class EnvironmentConfigSuite extends FunSuite {

    test("get with dev environment should return valid value") {

        EnvironmentConfig.set("dev")

        assert(EnvironmentConfig.get("master") == "local")
        assert(EnvironmentConfig.get("sourceSchema") == "test_spark")
        assert(EnvironmentConfig.get("targetSchema") == "test_spark")

    }

    test("get with prod environment should return valid value") {

        EnvironmentConfig.set("prod")

        assert(EnvironmentConfig.get("master") == "yarn")
        assert(EnvironmentConfig.get("sourceSchema") == "prod_spark")
        assert(EnvironmentConfig.get("targetSchema") == "prod_spark")

    }

}
