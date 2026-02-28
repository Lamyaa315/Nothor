import org.apache.spark.sql.SparkSession
import preprocessing.{Cleaning, Reduction, Transformation}

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("UK Road Safety - Big Data Pipeline")
      .master("local[*]")
      // توجيه الملفات المؤقتة لـ Spark إلى مجلد داخل المشروع
      // لتجنب مشكلة "No space left on device" على بعض الأجهزة
      .config("spark.local.dir", "./tmp")
      .config("spark.sql.shuffle.partitions", "4")
      .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")

    // إنشاء مجلد tmp إذا ما كان موجوداً
    new java.io.File("./tmp").mkdirs()

    // ─────────────────────────────────────────────
    // STEP 1 – READ RAW DATA
    // ─────────────────────────────────────────────
    println("\n========== STEP 1: READING RAW DATA ==========")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Accident_Information.csv")

    println(s"Rows loaded: ${df.count()}")
    df.printSchema()

    // ─────────────────────────────────────────────
    // STEP 2 – CLEANING  (شغل صديقتي – 5.1)
    // ─────────────────────────────────────────────
    println("\n========== STEP 2: CLEANING ==========")
    val cleanedDf = Cleaning.run(df)

    cleanedDf.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("data/cleaned_data")
    println("Saved: data/cleaned_data")

    // ─────────────────────────────────────────────
    // STEP 3 – SAMPLING 
    // ─────────────────────────────────────────────
    println("\n========== STEP 3: SAMPLING ==========")
    val sampledDf = Reduction.run(cleanedDf)

    sampledDf.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("data/sampled_data")
    println("Saved: data/sampled_data")

    // ─────────────────────────────────────────────
    // STEP 4 – REDUCTION  (Aggregation + Feature Selection)
    // ─────────────────────────────────────────────
    println("\n========== STEP 4: REDUCTION ==========")
    val reducedDf = Reduction.runReduction(sampledDf)

    reducedDf.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("data/reduced_data")
    println("Saved: data/reduced_data")

    // ────────────────────────────────────────────
    // STEP 5 – TRANSFORMATION  ( Type Conversion)
    // ─────────────────────────────────────────────
    println("\n========== STEP 5: TRANSFORMATION ==========")
    val transformedDf = Transformation.run(reducedDf)

    transformedDf.coalesce(1)
      .write.mode("overwrite")
      .option("header", "true")
      .csv("data/final_data")
    println("Saved: data/final_data")

    // ─────────────────────────────────────────────
    // SNAPSHOT – 15 rows from final dataset
    // ─────────────────────────────────────────────
    println("\n========== SNAPSHOT: Final Preprocessed Data (15 rows) ==========")
    transformedDf.show(15, truncate = false)

    println("\n========== PIPELINE COMPLETED SUCCESSFULLY ==========")
    println("Output files:")
    println("  -> data/cleaned_data   (after Cleaning)")
    println("  -> data/sampled_data   (after Sampling)")
    println("  -> data/reduced_data   (after Reduction)")
    println("  -> data/final_data     (after Transformation)")

    spark.stop()
  }
}
