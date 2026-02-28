package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Reduction {

  // ================================================================
  //لمياء  Sampling 
  // ================================================================
  def run(df: DataFrame): DataFrame = {

    // NOTE: Cleaning.scala converts all string columns to lowercase,
    //       so Accident_Severity values are: 'slight', 'serious', 'fatal'
    //       (not 'Slight', 'Serious', 'Fatal')

    // اعرف أقل فئة
    val minCount = df.groupBy("Accident_Severity")
      .count()
      .agg(org.apache.spark.sql.functions.min("count"))
      .collect()(0)(0)
      .toString.toLong

    println(s"[SAMPLING] Minimum class count: $minCount")

    // خذ نفس العدد من كل فئة (توازن كامل)
    // Using lowercase values because Cleaning.scala applies lower() to all string columns
    val slight   = df.filter("Accident_Severity = 'slight'").limit(minCount.toInt)
    val serious  = df.filter("Accident_Severity = 'serious'").limit(minCount.toInt)
    val fatal    = df.filter("Accident_Severity = 'fatal'").limit(minCount.toInt)

    val balancedDf = slight.union(serious).union(fatal)

    println(s"[SAMPLING] rows before: ${df.count()}")
    println(s"[SAMPLING] rows after: ${balancedDf.count()}")
    balancedDf.groupBy("Accident_Severity").count().show()

    balancedDf
  }

  // ================================================================
  // مها  – Aggregation + Feature Selection
  // ================================================================
  def runReduction(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println(s"\n[REDUCTION] Starting...")
    println(s"[REDUCTION] Rows before   : ${df.count()}")
    println(s"[REDUCTION] Columns before: ${df.columns.length}")

    // ──────────────────────────────────────────────────────────────
    // STEP 1 – AGGREGATION
    // ──────────────────────────────────────────────────────────────
    // WHY: Computing per-group summary statistics helps understand
    //      the data at a higher level and exposes patterns not visible
    //      at row level (e.g. which road type has the most casualties).
    // HOW: Group by Road_Type and Weather_Conditions, then compute:
    //        • total number of accidents
    //        • average number of casualties
    //        • average speed limit
    // IMPACT: Produces a summary table for the report.
    //         The main pipeline continues with row-level data for ML.

    println("\n[REDUCTION-AGGREGATION] Aggregating by Road_Type and Weather_Conditions...")

    val aggregatedDf = df
      .groupBy("Road_Type", "Weather_Conditions")
      .agg(
        count("*").alias("total_accidents"),
        round(avg("Number_of_Casualties"), 2).alias("avg_casualties"),
        round(avg("Speed_limit"), 2).alias("avg_speed_limit")
      )
      .orderBy(desc("total_accidents"))

    println("[REDUCTION-AGGREGATION] Top 10 groups:")
    aggregatedDf.show(10, truncate = false)
    println(s"[REDUCTION-AGGREGATION] Total groups: ${aggregatedDf.count()}")

    // ──────────────────────────────────────────────────────────────
    // STEP 2 – FEATURE SELECTION
    // ──────────────────────────────────────────────────────────────
    // WHY: Several columns are administrative identifiers or are
    //      redundant with other columns. Keeping them adds noise and
    //      increases memory/compute cost without improving model quality.
    // HOW: Drop the identified columns.
    // IMPACT: Column count reduced; remaining features are all relevant.

    val colsToDrop = Seq(
      "Location_Easting_OSGR",          // Redundant – covered by Latitude/Longitude
      "Location_Northing_OSGR",         // Redundant – covered by Latitude/Longitude
      "Police_Force",                    // Administrative ID, not a predictive feature
      "Local_Authority_(District)",      // Administrative ID, not a predictive feature
      "Local_Authority_(Highway)",       // Administrative ID, not a predictive feature
      "LSOA_of_Accident_Location",       // Very high-cardinality code, not useful for ML
      "Date",                            // Replaced by Date_std from Cleaning
      "Time"                             // Replaced by Time_std and Hour_of_Day
    ).filter(df.columns.contains)

    val reducedDf = df.drop(colsToDrop: _*)

    println(s"\n[REDUCTION-FEATURE_SELECTION] Columns dropped (${colsToDrop.length}): ${colsToDrop.mkString(", ")}")
    println(s"[REDUCTION-FEATURE_SELECTION] Columns before: ${df.columns.length}")
    println(s"[REDUCTION-FEATURE_SELECTION] Columns after : ${reducedDf.columns.length}")
    println(s"[REDUCTION-FEATURE_SELECTION] Remaining: ${reducedDf.columns.mkString(", ")}")

    // ──────────────────────────────────────────────────────────────
    // FINAL SUMMARY
    // ──────────────────────────────────────────────────────────────
    println(s"\n[REDUCTION] Completed.")
    println(s"[REDUCTION] Final rows   : ${reducedDf.count()}")
    println(s"[REDUCTION] Final columns: ${reducedDf.columns.length}")

    reducedDf
  }
}
