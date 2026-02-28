file:///C:/Users/tarfh/Nothor/Main.scala
empty definition using pc, found symbol in pc: 
semanticdb not found
empty definition using fallback
non-local guesses:
	 -sampledDf.
	 -sampledDf#
	 -sampledDf().
	 -scala/Predef.sampledDf.
	 -scala/Predef.sampledDf#
	 -scala/Predef.sampledDf().
offset: 1825
uri: file:///C:/Users/tarfh/Nothor/Main.scala
text:
```scala
import org.apache.spark.sql.SparkSession
import preprocessing.{Cleaning, Reduction, Transformation}
import java.io.PrintWriter
import java.io.File

object Main {
  def main(args: Array[String]): Unit = {

    System.setProperty("hadoop.home.dir", new java.io.File(".").getAbsolutePath)
    
    implicit val spark: SparkSession = SparkSession.builder()
      .appName("UK Road Safety - Big Data Pipeline")
      .master("local[*]")
      .config("spark.local.dir", "./tmp")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
      // Add Java 17 compatibility
      .config("spark.driver.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED")
      .config("spark.executor.extraJavaOptions", "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-exports=java.base/sun.util.calendar=ALL-UNNAMED")
      .getOrCreate()
 
    spark.sparkContext.setLogLevel("WARN")

    // Create output directory
    new File("./output").mkdirs()
    new File("./tmp").mkdirs()

    // ─────────────────────────────────────────────
    // STEP 1 – READ RAW DATA
    // ─────────────────────────────────────────────
    println("\n========== STEP 1: READING RAW DATA ==========")
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Accident_Information.csv")

    println(s"Rows loaded: ${df.count()}")

    // ─────────────────────────────────────────────
    // RUN ALL TRANSFORMATIONS
    // ─────────────────────────────────────────────
    println("\n========== RUNNING PIPELINE ==========")
    val cleanedDf = Cleaning.run(df)
    val sampledDf = Reduction.run(cleanedDf)
    val reducedDf = Reduction.runReduction(@@sampledDf)
    val transformedDf = Transformation.run(reducedDf)

    // ─────────────────────────────────────────────
    // SAVE DATA 
    // ─────────────────────────────────────────────
    println("\n========== SAVING TRANSFORMED DATA ==========")
    
    // Get columns list for reference
    val columns = transformedDf.columns
    println(s"Total columns: ${columns.length}")
    
    println("\nSaving full dataset with Spark write...")
    transformedDf.coalesce(1)
      .write
      .mode("overwrite")
      .option("header", "true")
      .option("delimiter", ",")
      .csv("output/transformed_data_full")
    
    println(s"✓ Full dataset saved to: output/transformed_data_full/")
    
    //save a small sample using show
    println("\n========== SAMPLE OF TRANSFORMED DATA ==========")
    transformedDf.show(10, truncate = false)
    
    println("\n========== COLUMN NAMES ==========")
    columns.grouped(5).foreach { group =>
      println(s"  ${group.mkString(", ")}")
    }

    val totalRows = transformedDf.count()
    println(s"\n Total rows: $totalRows")
    println(s"Total columns: ${columns.length}")

    println("\n" + "="*80)
    println("="*80)
    println("\nOutput folder: output/transformed_data_full/")
    println("Look for part-00000-*.csv file inside that folder")
    println("\nYour transformed dataset with all 41 columns is ready!")

    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 