package ru.etl.spark.utils

import org.scalatest.FunSuite


class ParametersSuite extends FunSuite {

    test("getParameter should return valid value") {
        val args = Array(
            "JOB_NM=test1",
            "TEST2==test2",
            "TEST3",
            "=test4",
            "TEST5 = test5",
            "TEST6= test6",
            "TEST7 =test7",
            "TEST8 = test8"
        )
        Parameters.initParameters(args)

        assert(Parameters.getParameter("JOB_NM") == "test1")
        assert(Parameters.getParameter("TEST2") == "")
        assert(Parameters.getParameter("TEST3") == "")
        assert(Parameters.getParameter("TEST4") == "")
        assert(Parameters.getParameter("TEST5") == "test5")
        assert(Parameters.getParameter("TEST6") == "test6")
        assert(Parameters.getParameter("TEST7") == "test7")
        assert(Parameters.getParameter("TEST8") == "test8")

    }

}
