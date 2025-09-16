package com.adp.datacloud.ds

import scala.xml.XML

object xmlTest {

  def main(args: Array[String]) {
    val resumeXMLNode = XML.loadFile("src/test/resources/mytest.xml")
    val PreviousPositions =
      (resumeXMLNode \\ "EmploymentHistory" \\ "PositionHistory").map { x =>
        (List(((x \ "Title").map { _.text } mkString "")) ++ ((x \\ "AnyDate").map {
          _.text
        })) mkString " ~~~ "
      }
    println(PreviousPositions)
  }

}
