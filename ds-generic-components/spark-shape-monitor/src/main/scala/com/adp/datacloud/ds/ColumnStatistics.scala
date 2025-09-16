package com.adp.datacloud.ds

import java.text.SimpleDateFormat
import java.util.Date
import org.apache.spark.sql.functions.lit

// Case class for a uni-variate statistics on a column, optionally filtered by a partition and sub-partition
// These objects shall be persisted in a rdbms table
case class ColumnStatistics (
    APPLICATION_ID:     String,
    DATABASE_NAME:      String,
    TABLE_NAME:         String,
    COLUMN_NAME:        String,                 // Column name for which "Statistics" are gathered. 
    COLUMN_TYPE:        String,                 // VIRTUAL OR PLAIN
    COLUMN_SPEC:        String = null,          // Only populated for Virtual Columns. Contains the spec of Virtual Column as defined in XML file.
    COLUMN_DATATYPE :   String,                 
    PARTITION_SPEC:     String = null,
    PARTITION_VALUE:    String = null,
    SUBPARTITION_SPEC:  String = null,
    SUBPARTITION_VALUE: String = null,
    // Statistics
    NUM_RECORDS:        Long,                   // Number of records
    NUM_MISSING:        Long,                   // Number of missing records. NULLs and Empty Strings are both considered missing.
    NUM_INVALID:        Long,                   // Number of invalid records. Only generated if list of valid values is provided in configuration (applicable only for categorical) 
    MEAN:               Option[Double] = None,  // Average value (applicable for numeric columns)
    STANDARD_DEVIATION: Option[Double] = None,  // Standard deviation (applicable for numeric columns)
    MIN:                Option[Double] = None,  // Minimum
    MEDIAN:             Option[Double] = None,  // Median (applicable only for numerics)
    FIFTH_PERCENTILE:   Option[Double] = None,  // 5th percentile (applicable only for numerics)
    FIRST_DECILE:       Option[Double] = None,  // 10th percentile (applicable only for numerics)
    FIRST_QUARTILE:     Option[Double] = None,  // 25th percentile (applicable only for numerics)
    THIRD_QUARTILE:     Option[Double] = None,  // 75th percentile (applicable only for numerics)
    NINTH_DECILE:       Option[Double] = None,  // 90th percentile (applicable only for numerics)
    NINTY_FIFTH_PERCENTILE:   Option[Double] = None, // 95th percentile (applicable only for numerics)
    MAX:                Option[Double] = None,
    NUM_APPROX_UNIQUE_VALUES: Long,             // approx_count_distinct
    DISTINCTNESS:       Double,
    ENTROPY:            Option[Double] = None,
    UNIQUE_VALUE_RATIO: Option[Double] = None,
    REC_CRT_DT:         String = new SimpleDateFormat("yyyyMMdd HHmmss").format(new Date)
    ) {
  
  private def roundAt(p: Int = 2)(n: Double): Double = { val s = math pow (10, p); (math round n * s) / s }
  
  // Additional derived measures
  def PCTG_MISSING: Option[Double] = {
    if (NUM_RECORDS == 0) None
    else Some(100*NUM_MISSING.toDouble/NUM_RECORDS.toDouble)
  }
  
  def PCTG_VALID:Option[Double] = {
    if (NUM_RECORDS == 0) None
    else Some(100* 1- (NUM_INVALID.toDouble/NUM_RECORDS.toDouble))
  }
  
 /* def UPPER_WHISKER {
    THIRD_QUARTILE + 1.5 * (THIRD_QUARTILE - FIRST_QUARTILE)
  }
  
  def LOWER_WHISKER {
    FIRST_QUARTILE - 1.5 * (THIRD_QUARTILE - FIRST_QUARTILE)
  }*/
  
}