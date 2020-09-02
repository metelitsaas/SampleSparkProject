package ru.etl.spark.utils


trait MainJob {
    def main(args: Array[String]): Unit = {
        // Main job template

        // Init job parameters
        Parameters.initParameters(args)
        EnvironmentConfig.set(Parameters.getParameter("ENV"))

        // Set SparkSession configuration class
        val appName = this.getClass.getSimpleName.replace("$", "")
        val master = EnvironmentConfig.get("master")

        val sparkSessionConfig = new SparkSessionConfig(appName, master)
        sparkSessionConfig.logger.info("SparkSession initialized")

        // Run job
        run(sparkSessionConfig)
    }

    /* Main job runner
    @param sparkSessionConfig: Config of SparkSession */
    def run(sparkSessionConfig: SparkSessionConfig): Unit
}