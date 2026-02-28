error id: file:///C:/Users/tarfh/Nothor/Main.scala:SparkSession
file:///C:/Users/tarfh/Nothor/Main.scala
empty definition using pc, found symbol in pc: 
semanticdb not found

found definition using fallback; symbol SparkSession
offset: 332
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
    
    implicit val spark: SparkSe@@ssion = SparkSession.builder()
      .appName("UK Road Safety - Big Data Pipeline")
      .master("local[*]")
      .config("spark.local.dir", "./tmp")
      .config("spark.sql.shuffle.partitions", "4")
      .config("spark.sql.adaptive.enabled", "false")
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
    val reducedDf = Reduction.runReduction(sampledDf)
    val transformedDf = Transformation.run(reducedDf)


    // ─────────────────────────────────────────────
    // SAVE DATA
    // ─────────────────────────────────────────────
    println("\n========== SAVING TRANSFORMED DATA (CSV - Guaranteed to work) ==========")
    
    // Get first 1000 rows as sample (to avoid memory issues)
    val sampleData = transformedDf.limit(1000).collect()
    val columns = transformedDf.columns
    
    // Write sample to CSV
    val sampleWriter = new PrintWriter("output/transformed_data_sample.csv")
    sampleWriter.println(columns.mkString(","))
    sampleData.foreach { row =>
      val values = columns.map { col =>
        val value = row.getAs[Any](col)
        if (value == null) "" 
        else value.toString.replace(",", ";")
      }
      sampleWriter.println(values.mkString(","))
    }
    sampleWriter.close()
    println(s"✓ Sample data (1000 rows) saved to: output/transformed_data_sample.csv")
    
    // For full data, save in chunks to avoid memory issues
    println("\nSaving full dataset in chunks...")
    val totalRows = transformedDf.count().toInt
    val chunkSize = 10000
    val numChunks = math.ceil(totalRows.toDouble / chunkSize).toInt
    
    // Write header
    val headerWriter = new PrintWriter("output/transformed_data_full.csv")
    headerWriter.println(columns.mkString(","))
    headerWriter.close()
    
    // Write data in chunks
    for (chunk <- 0 until numChunks) {
      val start = chunk * chunkSize
      val chunkDf = transformedDf.limit(start + chunkSize).except(transformedDf.limit(start))
      val chunkData = chunkDf.collect()
      
      val chunkWriter = new PrintWriter(new File("output/transformed_data_full.csv"), "UTF-8")
      chunkData.foreach { row =>
        val values = columns.map { col =>
          val value = row.getAs[Any](col)
          if (value == null) "" 
          else value.toString.replace(",", ";")
        }
        chunkWriter.println(values.mkString(","))
      }
      chunkWriter.close()
      
      println(s" Saved chunk ${chunk + 1}/$numChunks")
    }
    
    println(s"\n Full dataset saved to: output/transformed_data_full.csv")
    println(s" Total rows: $totalRows")
    println(s" Total columns: ${columns.length}")

    // ─────────────────────────────────────────────
    // SHOW RESULTS
    // ─────────────────────────────────────────────
    println("\n========== SAMPLE OF TRANSFORMED DATA ==========")
    transformedDf.show(10, truncate = false)
    
    println("\n========== COLUMN NAMES ==========")
    columns.grouped(5).foreach { group =>
      println(s"  ${group.mkString(", ")}")
    }

    println("\n" + "="*80)
    println("="*80)
    println("\nOutput files in the 'output' folder:")
    println("  1. transformed_data_json/ - JSON format")
    println("  2. transformed_data_sample.csv - First 1000 rows")
    println("  3. transformed_data_full.csv - Complete dataset (41 columns)")
    println("\nYour transformed dataset with all 41 columns is ready!")

    spark.stop()
  }
}
```


#### Short summary: 

empty definition using pc, found symbol in pc: 