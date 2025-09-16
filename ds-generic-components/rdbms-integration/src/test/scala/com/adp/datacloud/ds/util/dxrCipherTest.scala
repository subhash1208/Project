package com.adp.datacloud.ds.util

import java.util.regex.Pattern

object dxrCipherTest {

  def main(args: Array[String]) {
    println(
      com.adp.datacloud.ds.utils.dxrCipher
        .decrypt(com.adp.datacloud.ds.utils.dxrCipher.encrypt("password")) + ": password")

    println(List(("a", 1), ("a", 1), ("b", 2)).distinct)

    val testString2 =
      """-- SESSION_SETUP execute immediate 'alter session set "_with_subquery"=materialize';"""

    val testString =
      """
      
      -- SESSION_SETUP execute immediate 'alter session set "_with_subquery"=materialize';
      
         
      SELECT * FROM DUAL
      
         
      
      """

    val pattern = Pattern.compile("(?m)^\\s*\\-\\-\\s*SESSION_SETUP(.*)$")
    val matcher = pattern.matcher(testString)
    while (matcher.find()) {
      println(matcher.group(1))
    }
  }

}
