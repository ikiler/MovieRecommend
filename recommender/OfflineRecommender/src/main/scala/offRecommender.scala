/**
 * @author: create by 高岩
 * @description:
 * @date:2020/7/14
 **/

/**
 * userId,movieId,rating,timestamp
 */
case class MovieRating(userID: Int, movieID: Int, rating: Double, timestamp: Long)
/**
 * MongoDB COnfig
 *
 * @param uri mongo连接
 * @param db  数据库
 */
case class MongoConfig(uri: String, db: String)

//  基准推荐对象
case class Recommendation(moiveID: Int, score: Double)

//  定义基于预测评分的推荐列表
case class UserRects(uid: Int, recs: Seq[Recommendation])

//  定义基于LFM电影特征向量的推荐列表
case class MovieRects(mid: Int, recs: Seq[Recommendation])


class offRecommender {

  val  RATE_MORE_MOVIES = "RatiemoreMovies"
  val RATE_MORE_RENCETLY_MOVIES = "RatemoreRecentlyMovies"
  val AVEAGE_MOVIES = "AvgMovies"
  val GENRES_TOP_MOVIES = "GenresTopMovies"

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

  }


}
