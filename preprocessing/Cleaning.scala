package preprocessing

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Cleaning {

  /** Helper: count NULLs per column */
  private def nullCounts(df: DataFrame): DataFrame = {
    val exprs = df.columns.map(c => sum(col(c).isNull.cast("int")).alias(c))
    df.select(exprs: _*)
  }

  /** Helper: safe check column exists */
  private def hasCol(df: DataFrame, c: String): Boolean = df.columns.contains(c)

  /**
   * Step 5.1 (ONLY first 3 bullets):
   * 1) Missing values
   * 2) Errors & inconsistencies
   * 3) Duplicates
   *
   * Returns cleaned df, and prints before/after stats.
   */
  def run(dfRaw: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // =========================
    // BEFORE STATS
    // =========================
    val beforeRows = dfRaw.count()
    println(s"[BEFORE] rows = $beforeRows")
    println("[BEFORE] missing (null) counts per column (first 1 row shown):")
    nullCounts(dfRaw).show(1, truncate = false)

    // =========================
    // 1) MISSING VALUES
    // =========================
    // Case-by-case:
    // - Drop rows missing target (Accident_Severity) because can't be used for classification
    // - Fill missing categorical/text with "Unknown"
    // - Leave numeric NULLs as-is (or you can impute later if team wants, but not required now)

    val df1 =
      if (hasCol(dfRaw, "Accident_Severity")) dfRaw.filter(col("Accident_Severity").isNotNull)
      else dfRaw

    // Fill ONLY string columns with "Unknown"
    val stringCols = df1.schema.fields.collect { case StructField(name, StringType, _, _) => name }
    val df1Filled =
      if (stringCols.nonEmpty) df1.na.fill("Unknown", stringCols)
      else df1

    // =========================
    // 2) ERRORS & INCONSISTENCIES
    // =========================
    // Fix common issues safely (only if columns exist):
    // - Trim string columns (remove extra spaces)
    // - Remove impossible numeric values (basic validity checks)

    // 2a) Trim all string columns
    val df2Trimmed = stringCols.foldLeft(df1Filled) { (acc, c) =>
      acc.withColumn(c, when(col(c).isNull, lit(null)).otherwise(trim(col(c))))
    }

    // 2b) Basic validity rules (light, not aggressive)
    // Latitude/Longitude should be within valid earth ranges
    val df2Geo =
      if (hasCol(df2Trimmed, "Latitude") && hasCol(df2Trimmed, "Longitude")) {
        df2Trimmed.filter(
          col("Latitude").isNull || (col("Latitude") >= -90 && col("Latitude") <= 90)
        ).filter(
          col("Longitude").isNull || (col("Longitude") >= -180 && col("Longitude") <= 180)
        )
      } else df2Trimmed

    // Speed_limit should not be negative (UK common values: 20..70, but نخليه عام)
    val df2Speed =
      if (hasCol(df2Geo, "Speed_limit")) {
        df2Geo.filter(col("Speed_limit").isNull || (col("Speed_limit") >= 0 && col("Speed_limit") <= 150))
      } else df2Geo

    // Number_of_Vehicles / Number_of_Casualties should not be negative
    val df2Counts = Seq("Number_of_Vehicles", "Number_of_Casualties").foldLeft(df2Speed) { (acc, c) =>
      if (hasCol(acc, c)) acc.filter(col(c).isNull || col(c) >= 0) else acc
    }

    // =========================
    // 3) DUPLICATES
    // =========================
    // If Accident_Index exists, use it as unique accident id; otherwise drop full duplicates.
    val df3Deduped =
      if (hasCol(df2Counts, "Accident_Index")) df2Counts.dropDuplicates("Accident_Index")
      else df2Counts.dropDuplicates()

    // =========================
    // AFTER STATS
    // =========================
    val afterRows = df3Deduped.count()
    println(s"[AFTER] rows = $afterRows")
    println(s"[AFTER] removed rows = ${beforeRows - afterRows}")

    println("[AFTER] missing (null) counts per column (first 1 row shown):")
    nullCounts(df3Deduped).show(1, truncate = false)

    df3Deduped
  }
}