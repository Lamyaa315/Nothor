/**package preprocessing*/

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Cleaning {

  /** Safe column reference (handles special characters) */
  private def cc(name: String) = col(s"`${name}`")

  /** Column exists? */
  private def hasCol(df: DataFrame, name: String): Boolean = df.columns.contains(name)

  /** Count NULLs per column (returns a single-row DF) */
  private def nullCounts(df: DataFrame): DataFrame = {
    val exprs = df.columns.map(name => sum(cc(name).isNull.cast("int")).alias(name))
    df.select(exprs: _*)
  }

  /** Total NULLs across all columns (single number) */
  private def totalNulls(df: DataFrame): Long = {
    val row = nullCounts(df).collect()(0)
    df.columns.map(c => row.getAs[Long](c)).sum
  }

  /** Safe cast to Int */
  private def castToInt(df: DataFrame, name: String): DataFrame =
    if (hasCol(df, name)) df.withColumn(name, cc(name).cast(IntegerType)) else df

  /**
   * Cap outliers using IQR (winsorization).
   * Keeps all rows; replaces extreme values with lower/upper bounds.
   */
  private def capOutliersIQR(df: DataFrame, colName: String): DataFrame = {
    if (!hasCol(df, colName)) return df

    // ensure numeric for quantiles
    val dfNum = df.withColumn(colName, cc(colName).cast(DoubleType))

    val qs = dfNum.stat.approxQuantile(colName, Array(0.25, 0.75), 0.01)
    if (qs == null || qs.length < 2) return dfNum

    val q1 = qs(0)
    val q3 = qs(1)
    val iqr = q3 - q1
    val lower = q1 - 1.5 * iqr
    val upper = q3 + 1.5 * iqr

    dfNum.withColumn(
      colName,
      when(cc(colName).isNull, lit(null))
        .when(cc(colName) < lower, lit(lower))
        .when(cc(colName) > upper, lit(upper))
        .otherwise(cc(colName))
    )
  }

  /**
   * Phase 2 - 5.1 Data cleaning:
   * 1) Missing values
   * 2) Errors & inconsistencies
   * 3) Duplicates
   * 4) Outliers
   * 5) Standardization
   *
   * Prints before/after stats and per-step row removals.
   */
  def run(dfRaw: DataFrame)(implicit spark: SparkSession): DataFrame = {

    // =========================
    // BEFORE STATS
    // =========================
    val beforeRows = dfRaw.count()
    val beforeTotalNulls = totalNulls(dfRaw)

    println(s"[BEFORE] rows = $beforeRows")
    println(s"[BEFORE] total NULLs (all columns) = $beforeTotalNulls")
    println("[BEFORE] NULL counts per column (single row):")
    nullCounts(dfRaw).show(1, truncate = false)

    // =========================
    // 1) MISSING VALUES
    // =========================

    val rowsBeforeMissing = dfRaw.count()

    // 1a) Drop rows missing target label (Accident_Severity)
    val df1 =
      if (hasCol(dfRaw, "Accident_Severity")) dfRaw.filter(cc("Accident_Severity").isNotNull)
      else dfRaw

    val rowsAfterDropMissingLabel = df1.count()
    println(s"[MISSING] rows removed (missing Accident_Severity) = ${rowsBeforeMissing - rowsAfterDropMissingLabel}")

    // 1b) Fill missing string columns with "Unknown"
    val stringCols = df1.schema.fields.collect { case StructField(name, StringType, _, _) => name }

    val nullsBeforeFill = totalNulls(df1)

    val df1Filled =
      if (stringCols.nonEmpty) df1.na.fill("Unknown", stringCols)
      else df1

    val nullsAfterFill = totalNulls(df1Filled)
    val handledNullsByFill = nullsBeforeFill - nullsAfterFill

    println(s"[MISSING] NULLs handled by fill('Unknown' for String cols) = $handledNullsByFill")

    // =========================
    // 2) ERRORS & INCONSISTENCIES
    // =========================

    val rowsBeforeErrors = df1Filled.count()

    // 2a) Trim all string columns
    val df2Trimmed = stringCols.foldLeft(df1Filled) { (acc, name) =>
      acc.withColumn(name, when(cc(name).isNull, lit(null)).otherwise(trim(cc(name))))
    }

    // 2b) Validity filters (light rules)
    // Latitude/Longitude in valid ranges
    val df2Geo =
      if (hasCol(df2Trimmed, "Latitude") && hasCol(df2Trimmed, "Longitude")) {
        df2Trimmed
          .filter(cc("Latitude").isNull || (cc("Latitude") >= -90 && cc("Latitude") <= 90))
          .filter(cc("Longitude").isNull || (cc("Longitude") >= -180 && cc("Longitude") <= 180))
      } else df2Trimmed

    // Speed_limit >= 0 and <= 150 (broad)
    val df2Speed =
      if (hasCol(df2Geo, "Speed_limit")) {
        df2Geo.filter(cc("Speed_limit").isNull || (cc("Speed_limit") >= 0 && cc("Speed_limit") <= 150))
      } else df2Geo

    // Counts not negative
    val df2Counts = Seq("Number_of_Vehicles", "Number_of_Casualties").foldLeft(df2Speed) { (acc, name) =>
      if (hasCol(acc, name)) acc.filter(cc(name).isNull || cc(name) >= 0) else acc
    }

    val rowsAfterErrors = df2Counts.count()
    println(s"[ERRORS] rows removed (invalid values) = ${rowsBeforeErrors - rowsAfterErrors}")

    // =========================
    // 3) DUPLICATES
    // =========================

    val rowsBeforeDup = df2Counts.count()

    val df3Deduped =
      if (hasCol(df2Counts, "Accident_Index")) df2Counts.dropDuplicates("Accident_Index")
      else df2Counts.dropDuplicates()

    val rowsAfterDup = df3Deduped.count()
    println(s"[DUPLICATES] rows removed (duplicates) = ${rowsBeforeDup - rowsAfterDup}")

    // =========================
    // 4) OUTLIERS (CAPPING via IQR)
    // =========================
    val df4OutliersHandled = Seq("Speed_limit", "Number_of_Vehicles", "Number_of_Casualties").foldLeft(df3Deduped) {
      (acc, name) => capOutliersIQR(acc, name)
    }
    println("[OUTLIERS] Applied IQR capping on: Speed_limit, Number_of_Vehicles, Number_of_Casualties (no rows removed)")

    // =========================
    // 5) STANDARDIZATION
    // =========================

    // 5a) Lowercase all string columns (consistent categories)
    val df5Lower = stringCols.foldLeft(df4OutliersHandled) { (acc, name) =>
      acc.withColumn(name, when(cc(name).isNull, lit(null)).otherwise(lower(cc(name))))
    }

    // 5b) Parse Date into Date_std (UK often dd/MM/yyyy)
    val df5Date =
      if (hasCol(df5Lower, "Date")) {
        df5Lower.withColumn(
          "Date_std",
          coalesce(
            to_date(cc("Date"), "dd/MM/yyyy"),
            to_date(cc("Date"), "yyyy-MM-dd")
          )
        )
      } else df5Lower

    // 5c) Parse Time into Time_std (HH:mm or HH:mm:ss)
    val df5Time =
      if (hasCol(df5Date, "Time")) {
        df5Date.withColumn(
          "Time_std",
          coalesce(
            to_timestamp(cc("Time"), "HH:mm"),
            to_timestamp(cc("Time"), "HH:mm:ss")
          )
        )
      } else df5Date

    // 5d) Cast key numeric columns to integer
    val finalDf = Seq("Speed_limit", "Number_of_Vehicles", "Number_of_Casualties", "Year").foldLeft(df5Time) {
      (acc, name) => castToInt(acc, name)
    }

    // =========================
    // AFTER STATS
    // =========================
    val afterRows = finalDf.count()
    val afterTotalNulls = totalNulls(finalDf)

    println(s"[AFTER] rows = $afterRows")
    println(s"[SUMMARY] total rows removed overall = ${beforeRows - afterRows}")

    println(s"[AFTER] total NULLs (all columns) = $afterTotalNulls")
    println(s"[SUMMARY] total NULLs handled overall = ${beforeTotalNulls - afterTotalNulls}")

    println("[AFTER] NULL counts per column (single row):")
    nullCounts(finalDf).show(1, truncate = false)

    finalDf
  }
}