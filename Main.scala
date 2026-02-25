import org.apache.spark.sql.SparkSession

object Main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder()
      .appName("Accident Cleaning")
      .master("local[*]") 
      .getOrCreate()

    // اقرأ CSV
    val df = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv("data/Accident_Information.csv")

    println("Schema:")
    df.printSchema()

    // شغل التنظيف
    val cleanedDf = Cleaning.run(df)

    // احفظ الناتج
    cleanedDf.write
      .mode("overwrite")
      .option("header", "true")
      .csv("output/cleaned_data")

    println("Cleaning completed successfully ✅")

    spark.stop()
  }
}