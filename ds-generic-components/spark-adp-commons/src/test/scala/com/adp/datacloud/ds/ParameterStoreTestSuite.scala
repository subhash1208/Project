package com.adp.datacloud.ds

import com.adp.datacloud.ds.aws.ParameterStore
import com.adp.datacloud.util.BaseTestWrapper
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class ParameterStoreTestSuite extends AnyFunSuite with BaseTestWrapper {

  test("Querying a Non-Existing secret should return a None") {
    val paramValue = ParameterStore.getParameterByName("nonexisting", false)
    assert(paramValue.isEmpty)
  }

  test("Querying a valid string secret should a proper value") {
    val parameterName: String = "/DIT/__EMR_EC2_KEY_NAME__"
    val paramValue = ParameterStore
      .getParameterByName(parameterName, false)
    assert(paramValue == Some("cdlaws"))
  }

}
