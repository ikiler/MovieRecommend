import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.{MongoClient, MongoClientURI}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.elasticsearch.spark.rdd.EsSpark
import org.elasticsearch.spark.sql.EsSparkSQL

/**
 * @author: create by 高岩
 * @description:
 * @date:2020/6/27
 **/

/**
 * movieId,title,genres
 */
case class Movie(movieID: Int, title: String, genres: String)
case class MovieEs(movieID: Int, title: String, genres: String,tags:String)

/**
 * userId,movieId,rating,timestamp
 */
case class Rating(userID: Int, movieID: Int, rating: Double, timestamp: Long)

/**
 * userId,movieId,tag,timestamp
 */
case class Tag(userID: Int, movieId: Int, tag: String, timestamp: Long)

/**
 * MongoDB COnfig
 *
 * @param uri mongo连接
 * @param db  数据库
 */
case class MongoConfig(uri: String, db: String)

/**
 * ES Config
 *
 * @param httpHost      http主机列表 ，分割
 * @param transportHost transport 列表
 * @param index         索引
 * @param cluserName    集群名称
 */
case class EsConfig(httpHost: String, transportHost: String, index: String, cluserName: String)


object DataLoader {

  val MOVIE_DATA_PATH = "D:\\Projects\\java\\MovieRecommend\\recommender\\DataLoader\\src\\main\\resources\\movies.csv"
  val RATING_DATA_PATH = "D:\\Projects\\java\\MovieRecommend\\recommender\\DataLoader\\src\\main\\resources\\ratings.csv"
  val TAG_DATA_PATH = "D:\\Projects\\java\\MovieRecommend\\recommender\\DataLoader\\src\\main\\resources\\tags.csv"

  val MONGODB_TABLE_MOVIE = "Movie"
  val MONGODB_TABLE_RATING = "Rating"
  val MONGODB_TABLE_TAG = "Tag"
  val ES_INDEX = "Movie"

  val logRoot: Logger = Logger.getLogger(DataLoader.getClass)

  def main(args: Array[String]): Unit = {
    val config = Map(
      "saprk.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://airke.top:27017/recommender",
      "mongo.db" -> "recommender",
      "es.httphost" -> "airke.top:9200",
      "es.transportHost" -> "airke.top:9300",
      "es.index" -> "·recommender",
      "cluster.name" -> "elasticsearch"
    )
    //    创建sparkconf
    val sparkConf = new SparkConf()
      .setMaster(config("saprk.cores"))
      .setAppName("DataLoader")
      .set("cluster.name", "elasticsearch")
      .set("es.index.auto.create", "true")
      .set("es.nodes", "airke.top")
      .set("es.port", "9200")
      .set("es.index.read.missing.as.empty", "true")
      .set("es.nodes.wan.only", "true")

    //    创建sparksession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    //      加载数据
    val movieRDD = sparkSession.sparkContext.textFile(MOVIE_DATA_PATH)
    val ratingRDD = sparkSession.sparkContext.textFile(RATING_DATA_PATH)
    val tagRDD = sparkSession.sparkContext.textFile(TAG_DATA_PATH)
    import sparkSession.implicits._

    val movieDF = movieRDD.map(
      item => {
        val attr = item.split(",")
        Movie(attr(0).toInt, attr(1).trim, attr(2).trim)
      }
    ).toDF()

    val ratingDF = ratingRDD.map(
      item => {
        val attr = item.split(",")
        Rating(attr(0).toInt, attr(1).toInt, attr(2).toDouble, attr(3).toLong)
      }
    ).toDF()
    val tagDF = tagRDD.map(
      item => {
        val attr = item.split(",")
        Tag(attr(0).toInt, attr(1).toInt, attr(2).trim, attr(3).toLong)
      }
    ).toDF()

    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
//    implicit val esConfig = EsConfig(config("es.httphost"), config("es.transportHost"),
//      config("es.index"), config("es.cluster.name"))

    //    数据预处理
    saveDataTOMongoDB(movieDF, ratingDF, tagDF)

    //    处理movie的tag聚合为一条数据
    import org.apache.spark.sql.functions._
    val newTag = tagDF.groupBy($"movieID")
      .agg(concat_ws("|"), collect_set($"tag").as("tags")) //多个tag聚合为一条，以 | 分割
      .select("movieID", "tags") //只选择这两个数据
    //    newtag和movie连接到一起
    val movieJoinTagsDF = movieDF.join(newTag, Seq("movieID"), "left")

    saveDataToES(movieJoinTagsDF,sparkSession.sparkContext)
    sparkSession.stop()
  }

  def saveDataTOMongoDB(movieDF: DataFrame, ratingDF: DataFrame, tagDF: DataFrame)(implicit mongoConfig: MongoConfig): Unit = {
    //    新建mongodb链接
    val mongoClient = MongoClient(MongoClientURI(mongoConfig.uri))
    //if exits then drop
    mongoClient(mongoConfig.db)(MONGODB_TABLE_MOVIE).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TABLE_RATING).dropCollection()
    mongoClient(mongoConfig.db)(MONGODB_TABLE_TAG).dropCollection()
    //写入到数据库
    movieDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TABLE_MOVIE)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    ratingDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TABLE_RATING)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    tagDF.write
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TABLE_TAG)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
    //建立索引
    mongoClient(mongoConfig.db)(MONGODB_TABLE_MOVIE).createIndex(MongoDBObject("movieID" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TABLE_RATING).createIndex(MongoDBObject("userID" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TABLE_RATING).createIndex(MongoDBObject("movieID" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TABLE_TAG).createIndex(MongoDBObject("userID" -> 1))
    mongoClient(mongoConfig.db)(MONGODB_TABLE_TAG).createIndex(MongoDBObject("movieID" -> 1))

    mongoClient.close()
  }

  def saveDataToES(movieJoinTagsDF: DataFrame, spark:SparkContext): Unit = {
    val MovieOnEs:Dataset[Row] = movieJoinTagsDF.as("MovieEs")
    EsSparkSQL.saveToEs(MovieOnEs,"movie")
//    EsSpark.saveToEs(movieJoinTagsDF.toJavaRDD,"movie")

  }
}
