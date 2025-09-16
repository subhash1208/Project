package com.adp.datacloud.ds

import com.adp.datacloud.ds.udfs.UDFUtility.strSanitize
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

// @RunWith(classOf[JUnitRunner])
class StringSanatizeTestSuite extends AnyFunSuite {

  test("Null strings should produce none") {
    assert(strSanitize(null).isEmpty)
  }

  test("Acronym with multiple expansion candidates should pick the first one") {
    assert(strSanitize("acctg clerk").get == "accounting clerk")
    assert(strSanitize("AVP technology").get == "assistant vice president technology")
  }

  test("String with roman numerals") {
    assert(strSanitize("Software Engineer II").get == "Software Engineer")
  }

  test("Tokens which are entirely made up of number should be dropped") {
    println(strSanitize("Software.Engineer 123123").get)
    assert(strSanitize("Software.Engineer 123123").get == "Software Engineer")
  }

  test("Ampersand should not be used for tokenization, but dot should be used") {
    assert(strSanitize("F&B.Manager").get == "F&B Manager")
  }

  test("Ampersand should not be used for tokenization, but dot should be used") {
    println(strSanitize("S.V.P").get == "SVP")
  }
}
