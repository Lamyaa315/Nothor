import org.apache.spark.sql.DataFrame

object Reduction {
  def run(df: DataFrame): DataFrame = {

    // اعرف أقل فئة
    val minCount = df.groupBy("Accident_Severity")
      .count()
      .agg(org.apache.spark.sql.functions.min("count"))
      .collect()(0)(0)
      .toString.toLong

    println(s"[SAMPLING] Minimum class count: $minCount")

    // خذ نفس العدد من كل فئة (توازن كامل)
    val slight   = df.filter("Accident_Severity = 'Slight'").limit(minCount.toInt)
    val serious  = df.filter("Accident_Severity = 'Serious'").limit(minCount.toInt)
    val fatal    = df.filter("Accident_Severity = 'Fatal'").limit(minCount.toInt)

    val balancedDf = slight.union(serious).union(fatal)

    println(s"[SAMPLING] rows before: ${df.count()}")
    println(s"[SAMPLING] rows after: ${balancedDf.count()}")
    balancedDf.groupBy("Accident_Severity").count().show()

    balancedDf
  }
}