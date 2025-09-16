package com.adp.datacloud.ds

import com.adp.datacloud.ds.aws.SecretsStore
import com.adp.datacloud.util.BaseTestWrapper
import org.junit.runner.RunWith
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class SecretsManagerTestSuite extends AnyFunSuite with BaseTestWrapper {

  test("Querying a Non-Existing secret should return a None") {
    assert(SecretsStore.getSecretByName("test").isEmpty)
  }

  test("Querying a valid string secret should a proper value") {
    assert(
      SecretsStore
        .getSecretByName("unit-test-credentials")
        .get == """{"testusername":"testpassword"}""")
  }

  test("Querying for credentials from a map should return a proper value") {
    assert(
      SecretsStore
        .getCredential("unit-test-credentials", "testusername")
        .get == "testpassword")
  }

  test("Querying for nonexistent credentials from a map should return None") {
    assert(SecretsStore.getCredential("unit-test-credentials", "invalidusername").isEmpty)
  }

}
