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

}
