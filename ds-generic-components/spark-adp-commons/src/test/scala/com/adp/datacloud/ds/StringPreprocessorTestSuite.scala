package com.adp.datacloud.ds

import com.adp.datacloud.ds.udfs.UDFUtility
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

// @RunWith(classOf[JUnitRunner])
class StringPreprocessorTestSuite extends AnyFunSuite {

  test("String preprocessor should not fail on invalid input") {
//    assert(UDFUtility.strSanitize("...").isEmpty())
//    assert(UDFUtility.strSanitize(".").isEmpty())
//    assert(UDFUtility.strSanitize("@").isEmpty())
//    assert(UDFUtility.strSanitize(null).isEmpty())
//    assert(UDFUtility.strSanitize("").isEmpty())
  }

  test("String preprocessor should not fail on valid input") {
    println(UDFUtility.strSanitize("C01 SrRegCrdtAnl A90"))
    println(UDFUtility.strSanitize("Maintenance Planner / Schedule"))
    println(UDFUtility.strSanitize("Assoc. GTM Electrical Technici"))
    println(UDFUtility.strSanitize("VP of Eng & Chief Tech Officer"))
    println(UDFUtility.strSanitize("A111 Crop Advisor2"))
    println(UDFUtility.strSanitize("ZZZ Cyborg III"))
    println(UDFUtility.strSanitize("processsor"))
    println(UDFUtility.strSanitize("CNA"))
    println(UDFUtility.strSanitize("prod svc"))
    true
  }

}
