package ru.etl.spark.utils


object Parameters {
    // Spark submit parameters

    private var parametersMap: Map[String, String] = _

    /* Initialize parameters
    @param args: Arguments of job */
    def initParameters(args: Array[String]): Unit = {
        parametersMap = args
                .map(_.split("="))
                .filter(_.length == 2)
                .map(arg => (arg(0).trim, arg(1).trim))
                .toMap[String, String]
                .filter { case (key, value) => value != null &&
                    value.nonEmpty &&
                    key != null &&
                    key.nonEmpty }
    }

    /* Get parameter from parameters map
    @param name: Name of parameter
    @param defaultValue: Default value of parameter
    @result : Value of parameter */
    def getParameter(name: String, defaultValue: String = ""): String = {
        parametersMap.getOrElse(name, defaultValue)
    }

}