import java.text.SimpleDateFormat
import java.util.Date

import com.mongodb.casbah.{MongoClient, MongoClientURI}
import com.mongodb.casbah.commons.MongoDBObject
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 * @author: create by 高岩
 * @description:
 * @date:2020/6/28
 **/


/**
 * movieId,title,genres
 */
case class Movie(movieID: Int, title: String, genres: String)

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

//  基准推荐对象
case class Recommendation(moiveID: Int, score: Double)

//  电影类别top推荐
case class GenressRecommend(genres: String, recs: Seq[Recommendation])



object Recommender {

  val MONGODB_TABLE_MOVIE = "Movie"
  val MONGODB_TABLE_RATING = "Rating"

  //统计表名称
  val POPULAR_MOVIE = "PopularMovie"
  val HOT_RECENTLY = "HotRecently"
  val AVG_MOVIE = "Avg_Movie"
  val GENERS_MOVIE = "Geners_Movie"

  def main(args: Array[String]): Unit = {
    val config = Map(
      "saprk.cores" -> "local[*]",
      "mongo.uri" -> "mongodb://airke.top:27017/recommender",
      "mongo.db" -> "recommender"
    )
    implicit val mongoConfig = MongoConfig(config("mongo.uri"), config("mongo.db"))
    //    创建sparkconf
    val sparkConf = new SparkConf()
      .setMaster(config("saprk.cores"))
      .setAppName("Recommender")

    //    创建sparksession
    val sparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
    import sparkSession.implicits._

    //    从mongodb获取数据
    val ratingDS = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TABLE_RATING)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Rating]
    val movieDS = sparkSession.read
      .option("uri", mongoConfig.uri)
      .option("collection", MONGODB_TABLE_MOVIE)
      .format("com.mongodb.spark.sql")
      .load()
      .as[Movie]

    ratingDS.createOrReplaceTempView("ratings")
//        movieDS.createOrReplaceTempView("movies")

    //    历史热门--评分最多
    val popMovieDS = sparkSession.sql("select movieID,count(movieID) as count from ratings group by movieID")
    storeInMongoDB(popMovieDS, POPULAR_MOVIE)

    //    近期热门--评分个数
    val simpleDateFormat = new SimpleDateFormat("yyyyMM")
    sparkSession.udf.register("changeDate", (x: Int) => simpleDateFormat.format(new Date(x)).toInt)
    val ratingYearOfMonth = sparkSession.sql("select movieID,rating,changeDate(timestamp) as yearmonth from ratings")
    ratingYearOfMonth.createOrReplaceTempView("ratingOfMonth")
    val hotRecentlyDS = sparkSession.sql(
      "select movieID,count(movieID) as count,yearmonth from ratingOfMonth group by yearmonth,movieID order by yearmonth desc,count desc")
    storeInMongoDB(hotRecentlyDS, HOT_RECENTLY)

    //    优质电影--平均评分
    val avgMovie = sparkSession.sql("select movieID,avg(rating) as rating from ratings group by movieID")
    storeInMongoDB(avgMovie, AVG_MOVIE)

    //    个类别电影统计
    //    定义类别
    val genres = List(
      "Action","Adventure","Animation","Children\'s","Comedy", "Crime",
      "Documentary", "Drama", "Fantasy", "Film-Noir", "Horror", "Musical",
      "Mystery", "Romance", "Sci-Fi", "Thriller", "War", "Western"
    )
    val movieWithScore = movieDS.join(avgMovie,"movieID")
    val genersRDD = sparkSession.sparkContext.makeRDD(genres)
//    计算类别top10首先对类别和电影做笛卡尔积
    val genresTopDS = genersRDD.cartesian(movieWithScore.rdd)
        .filter{
//              条件过滤，找出movie字段genres值包含genre字段有哪些
          case (genre,moiveRow)=>moiveRow.getAs[String]("genres").toLowerCase().contains(genre.toLowerCase())
        }
        .map{
          case (genre,moiveRow)=>(genre,(moiveRow.getAs[Int]("movieID"),moiveRow.getAs[Double]("rating")))
        }
        .groupByKey()
        .map{
          case (genre,items)=>GenressRecommend(genre,items.toList.sortWith(_._2>_._2).take(10)
          .map(item=>Recommendation(item._1,item._2)))
        }
        .toDF()
        storeInMongoDB(genresTopDS, GENERS_MOVIE)



    sparkSession.stop()
  }

  def storeInMongoDB(db: DataFrame, collection_name: String)(implicit mongoConfig: MongoConfig): Unit = {
    db.write
      .option("uri", mongoConfig.uri)
      .option("collection", collection_name)
      .mode("overwrite")
      .format("com.mongodb.spark.sql")
      .save()
  }
}
