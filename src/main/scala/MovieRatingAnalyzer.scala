import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, stddev_pop}

object MovieRatingAnalyzer {
  val spark = SparkSession
    .builder
    .appName("Movie Rating Analyzer")
    .master("local[*]")
    .getOrCreate()

  val movieData = spark.read.option("inferSchema", "true")
    .option("header", "true")
    .csv("src/main/resources/movie_metadata.csv")

  def main(args: Array[String]): Unit = {
    val stats = movieData.agg(
      avg("imdb_score").alias("average_imdb_score"),
      stddev_pop("imdb_score").alias("stddev_imdb_score"))
    stats.show()
  }
}
