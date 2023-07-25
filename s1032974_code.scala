package org.rubigdata
//Alexandru Ciutacu - s1032974

// Importing necessary libraries
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.io.NullWritable
import de.l3s.concatgz.io.warc.{WarcGzInputFormat,WarcWritable}
import de.l3s.concatgz.data.WarcRecord
import org.apache.spark.SparkConf
import org.jsoup.Jsoup
import org.jsoup.nodes.{Document,Element}
import collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql._

// Main object
object RUBigDataApp {
  def main(args: Array[String]) {
    // Defining Spark configuration
    val sparkConf = new SparkConf()
      .setAppName("RUBigDataApp")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .registerKryoClasses(Array(classOf[WarcRecord]))
      .set("spark.driver.memory", "3g")
      .set("spark.executor.memory", "5g")
      .set("spark.memory.offHeap.enabled", "true")
      .set("spark.memory.offHeap.size", "4g")

    // Starting a Spark session
    implicit val spark = SparkSession.builder.config(sparkConf).getOrCreate()
    val sc = spark.sparkContext

    // Generating array of WARC file paths, now considering 50 files
    val warcFiles = (0 to 49).map(a => s"hdfs:/single-warc-segment/CC-MAIN-20210410105831-20210410135831-%05d.warc.gz".format(a))

    // Processing the WARC files
    val urlWordCount = sc.newAPIHadoopFile(
      warcFiles.mkString(","),
      classOf[WarcGzInputFormat],
      classOf[NullWritable],
      classOf[WarcWritable]
    )
    .map{ wr => wr._2.getRecord()}  // Mapping to the record
    .filter(wr => wr.getHeader().getUrl() != null && wr.getHttpStringBody() != null)  // Filtering out null URL or body
    .map{ wr => {  // Counting matches with the pattern
      val d = Jsoup.parse(wr.getHttpStringBody())
      val b = d.select("body").text()
      val pattern = "\\b(([Ff]itness)|([Ee]xercise)|([Ww]orkout)|([Nn]utrition)|([Hh]ealth)|([Ww]ellness)|([Ww]eight [Ll]oss)|([Ss]trength [Tt]raining)|([Cc]ardio)|([Bb]odybuilding))\\b".r
      val patternCount = pattern.findAllIn(b).toList.length
      (wr.getHeader().getUrl(), patternCount)
    }}
    .filter(r => r._2 > 0)  // Filtering out pages without matches

    // Converting to DataFrame
    val df = spark.createDataFrame(urlWordCount).toDF("url", "wordcount")
    df.createOrReplaceTempView("df")

    // Querying the DataFrame
    val result = spark.sql("SELECT url, wordcount FROM df ORDER BY wordcount DESC LIMIT 5").take(5)

    // Printing the results
    println("\n########## OUTPUT ##########")
    println("Top 5 webpages in Warc-files:")
    result.foreach(println)
    println("########### END ############\n")

    // Stopping the Spark session
    spark.stop()
  }
}

