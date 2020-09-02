package ru.etl.spark.utils

import com.typesafe.config.{Config, ConfigFactory}


object EnvironmentConfig {

    // Constants
    private val CONFIG_FILE = "environment.conf"
    private val PRODUCTION = "prod"
    private val DEVELOPMENT = "dev"

    // Reading config
    private val config: Config = ConfigFactory.parseResources(CONFIG_FILE)

    // Environment
    private var environment: String = _

    /* Get configuration parameter by key
    @param environment: Environment of SparkSession
    @param value: Name of parameter
    @return : Value of parameter */
    private def getConfig(environment: String, value: String): String = {
        config.getObject(environment).get(value).unwrapped().toString
    }

    /* Set environment
    @param value: Name of environment */
    def set(value: String): Unit = environment = value

    /* Get environment parameter by key
    @param key: Name of parameter
    @return : Value of parameter */
    def get(key: String): String = environment match {
            case PRODUCTION => getConfig(PRODUCTION, key)
            case DEVELOPMENT => getConfig(DEVELOPMENT, key)
    }

}