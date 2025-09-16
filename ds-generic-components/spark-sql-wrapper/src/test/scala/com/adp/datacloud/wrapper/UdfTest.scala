package com.adp.datacloud.wrapper

import com.adp.datacloud.ds.udfs.UDFUtility
import org.apache.spark.sql.SparkSession

object UdfTest {

  def main(args: Array[String]) = {

    implicit val sparkSession = SparkSession.builder
      .appName("Spark SQL Stats Generator")
      .master("local")
      .getOrCreate()

    UDFUtility.registerUdfs
    //md5UdfTest
    stateUdiTest

  }

  def stateUdiTest(implicit sparkSession: SparkSession) {

    /*select udi, derive_udistate(udi) as derived_udi from
      (select
      map(key,array(named_struct('etype',etype,'ttype','sdi','amt',amt),
      named_struct('etype',etype2,'ttype','sdi_tb','amt',amt2)),'CA',array(named_struct('etype',etype2,'ttype',ttype2,'amt',cast(2000.00 as double)),named_struct('etype',etype2,'ttype',ttype2,'amt',cast (4000.01 as double))))
      as udi
      from
      (select
      'KY' as key,
      'er' as etype,
      'sui' as ttype,
       cast(23.00 as double) as amt,
      'ee' as etype2,
      'sui_tb' as ttype2,
       cast(200.00 as double) as amt2 ) x)y
       union all

     */
    // [KY -> [[er, sui, 15.7855], [er, sui_tb, 1578.55]]]

    val x = sparkSession.sql("""
       
       select map('',null)as udi, 
              derive_udistate(map('',null)) as derived_udi,
              derive_udistate(null) as nullpointer_udi""")

    x.show(100, false)
    x.printSchema
  }

  def md5UdfTest(implicit sparkSession: SparkSession) {

    sparkSession
      .sql("""	select x.*,
	md5_udf(coalesce(ooid,'')||coalesce(aoid,'')||coalesce(ssn,'')||coalesce(birthdate,'')) as guid,
	length(md5_udf(coalesce(ooid,'')||coalesce(aoid,'')||coalesce(ssn,'')||coalesce(birthdate,''))) as length
	from (
	select
	'XXXX59SHD2K0XXXX' as ooid,
	'XXXXBV5QACK0XXXX' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'73770678FCC3185FD5DE6B45EDD10CA384767F9DFE480EAC54FEB0360A67BDE1' as birthdate
	union all
	select
	null as ooid,
	'XXXXBV5QACK0XXXX' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'73770678FCC3185FD5DE6B45EDD10CA384767F9DFE480EAC54FEB0360A67BDE1' as birthdate
	union all
	select
	'XXXX59SHD2K0XXXX' as ooid,
	null as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'73770678FCC3185FD5DE6B45EDD10CA384767F9DFE480EAC54FEB0360A67BDE1' as birthdate
	union all
	select
	'XXXX59SHD2K0XXXX' as ooid,
	'XXXXBV5QACK0XXXX' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	null as birthdate
	union all 
	select
	'' as ooid,
	'XXXXBV5QACK0XXXX' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'73770678FCC3185FD5DE6B45EDD10CA384767F9DFE480EAC54FEB0360A67BDE1' as birthdate
	union all
	select
	'XXXX59SHD2K0XXXX' as ooid,
	'' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'73770678FCC3185FD5DE6B45EDD10CA384767F9DFE480EAC54FEB0360A67BDE1' as birthdate
	union all
	select
	'XXXX59SHD2K0XXXX' as ooid,
	'XXXXBV5QACK0XXXX' as aoid,
	'653AC142D89AA0F0125FDB1B9251079670EE2ADBA198C412923649FEBE79BBC9' as ssn,
	'' as birthdate
	union all
	select 
  'B500267812800397' as ooid,
  '050E0FT1EBM0BP0E' as aoid,
  'A727BDFC3AC0B3EE079648D8A0A1F840DD2A1438AA7E708C57AC0378F211E12D' as ssn,
  'D283517000C721E9DBCEE8920D4F9446E8437A9E6E81A4F5313825174D325D87' as birthday) x """)
      .show(10, false)

  }

}
