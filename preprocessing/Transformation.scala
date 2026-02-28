package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Transformation {

  /**

   *
   * Note: Cleaning.scala already handled:
   *   - String → Integer  (Speed_limit, Number_of_Vehicles, Number_of_Casualties, Year)
   *   - String → Date     (Date → Date_std)
   *   - String → Timestamp (Time → Time_std)
   *
   * This step adds:
   *   1) String → Double  (Latitude, Longitude)
   *   2) Timestamp → Hour_of_Day Integer  (extracted from Time_std)
   */
  def run(df: DataFrame)(implicit spark: SparkSession): DataFrame = {

    println("\n[TRANSFORMATION] Starting...")
    println(s"[TRANSFORMATION] Rows   : ${df.count()}")
    println(s"[TRANSFORMATION] Columns: ${df.columns.length}")

    // =========================================================
    // STEP 1 – Cast Latitude and Longitude to Double
    // =========================================================
    // WHY: Latitude and Longitude were read as String by inferSchema
    //      because some rows contain non-numeric placeholders.
    //      Casting to Double makes them usable in distance calculations
    //      and as numeric ML features.
    // HOW: Use .cast(DoubleType); invalid strings become null.

    println("\n[TRANSFORMATION] Step 1 – Cast Latitude & Longitude to Double")

    val df1 = df
      .withColumn("Latitude",  col("Latitude").cast(DoubleType))
      .withColumn("Longitude", col("Longitude").cast(DoubleType))

    val latNulls = df1.filter(col("Latitude").isNull).count()
    val lonNulls = df1.filter(col("Longitude").isNull).count()
    println(s"[TRANSFORMATION]   Latitude  nulls after cast : $latNulls")
    println(s"[TRANSFORMATION]   Longitude nulls after cast : $lonNulls")

    // =========================================================
    // STEP 2 – Extract Hour_of_Day from Time_std
    // =========================================================
    // WHY: The raw Time column (e.g. "17:42") was already parsed to a
    //      Timestamp in Cleaning (Time_std). Extracting the integer hour
    //      (0–23) creates a numeric feature that captures time-of-day
    //      patterns (rush hour, night-time, etc.) for ML models.
    // HOW: Apply Spark's hour() function on Time_std.

    println("\n[TRANSFORMATION] Step 2 – Extract Hour_of_Day from Time_std")

    val df2 =
      if (df1.columns.contains("Time_std")) {
        df1.withColumn("Hour_of_Day", hour(col("Time_std")).cast(IntegerType))
      } else {
        println("[TRANSFORMATION]   WARNING: Time_std column not found – skipping Hour_of_Day extraction")
        df1
      }

    val hourNulls = df2.filter(col("Hour_of_Day").isNull).count()
    println(s"[TRANSFORMATION]   Hour_of_Day nulls: $hourNulls")
    println("[TRANSFORMATION]   Sample Hour_of_Day distribution:")
    df2.groupBy("Hour_of_Day").count().orderBy("Hour_of_Day").show(24, truncate = false)

    // =========================================================
    // FINAL SUMMARY
    // =========================================================
    println(s"\n[TRANSFORMATION] Completed.")
    println(s"[TRANSFORMATION] Final rows   : ${df2.count()}")
    println(s"[TRANSFORMATION] Final columns: ${df2.columns.length}")
    println(s"[TRANSFORMATION] New columns added: Latitude (Double), Longitude (Double), Hour_of_Day (Int)")

    // ================================================================
    //   طرفه ضيفي كودك بعد هذا الكومنت
    // ================================================================
   
    df2
  }
}
