package ai.economicdatasciences.sia.sql

import ai.economicdatasciences.sia.spark.SparkInit.{sc, spark}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import spark.implicits._

import ai.economicdatasciences.sia.model.StringImplicits._
import ai.economicdatasciences.sia.model.Post

class WorkWithDF {

  // get the data
  val itPostsRows = sc.textFile("data/italianPosts.csv")
  // split the data
  val itPostsSplit = itPostsRows.map(x => x.split("~"))

  // make into RDD
  val itPostsRDD = itPostsSplit.map(x => {
    (x(0), x(1), x(2), x(3), x(4), x(5), x(6), x(7), x(8), x(9), x(10), x(11), x(12))
  })
  // to dataframe
  // val itPostsDF = itPostsRDD.toDF("commentCount", "lastActivityDate", "ownerUserId", "body", "score", "creationDate", "viewCount", "title", "tags", "answerCount", "acceptedAnswerId", "postTypeId", "id")
  // function to change string to posts
  def stringToPost(row: String): Post = {
    val r = row.split("~")
    Post(
    r(0).toIntSafe,
    r(1).toInstantSafe,
    r(2).toLongSafe,
    r(3),
    r(4).toIntSafe,
    r(5).toInstantSafe,
    r(6).toIntSafe,
    r(7),
    r(8),
    r(9).toIntSafe,
    r(10).toLongSafe,
    r(11).toLongSafe,
    r(12).toLong
    )
  }

  // make a DF
  val itPostsDF = itPostsRows.map(x => stringToPost(x)).toDF
  // select
  val postsIdBody = itPostsDF.select("id","body")
  // some answers
  val exp1 = postsIdBody.filter('body contains "Italiano").count
  // comparing to my preferred method
  postsIdBody.createOrReplaceTempView("idbod")
  val exp1Alt = spark.sql("SELECT * FROM idbod WHERE body like '%Italiano%'").count
  // comparing a filter method
  val exp2 = itPostsDF.filter('postTypeId === 1).withColumn("ratio", 'viewCount / 'score).where('ratio < 35).show()
  // no preferred
  itPostsDF.createOrReplaceTempView("posts")
  val exp2Alt = spark.sql("""
    SELECT * FROM (
      SELECT *, viewCount/score as ratio FROM posts
    ) as a
    WHERE postTypeId == 1 AND ratio < 35
    """
  )

  // using spark sql functions
  val exp3 = itPostsDF.select(avg("score"), max("score"), count("score")) //.show()
  // preferred
  val exp3Alt = spark.sql("SELECT AVG(score), MAX(score), COUNT(score) FROM posts")//.show()
  //
  val exp4 = itPostsDF.filter('postTypeId === 1).withColumn("activePeriod", datediff('lastActiveDate, 'createDate)).orderBy('activePeriod desc).head.getString(3)
  // window functions
  val exp5 = itPostsDF.filter($"postTypeId" === 1).select($"ownerUserId", $"acceptedAnswerId", $"score", max($"score").over(Window.partitionBy($"ownerUserId")) as "maxPerUser").withColumn("toMax", $"maxPerUser" - $"score").show(10)
  // pure sql
  val exp5Alt = spark.sql(
    """
    SELECT *, (maxPerUser - score) as toMax FROM (
      SELECT ownerUserId, acceptedAnswerId, score, MAX(score) OVER( PARTITION BY ownerUserId) as maxPerUser FROM posts
    ) as a
    """
  )
  // using lag/lead
  val exp6 = itPostsDF.filter($"postTypeId" === 1).select($"ownerUserId", $"id", $"creationDate", lag($"id", 1).over(
    Window.partitionBy($"ownerUserId").orderBy($"creationDate")
  ) as "prev", lead($"id", 1).over(
    Window.partitionBy($"ownerUserId").orderBy($"creationDate")
  ) as "next").orderBy($"ownerUserId", $"id")
  // pure sql
  val exp6Alt = spark.sql(
    """
    SELECT
      ownerUserId
      ,id
      ,creationDate
      ,LAG(id, 1) OVER( PARTITION BY ownerUserId ORDER BY creationDate) as prev
      ,LEAD(id, 1) OVER( PARTITION BY ownerUserId ORDER BY creationDate) as next
    FROM posts
    WHERE postTypeId = 1
    ORDER BY ownerUserId, id
    """
  )

  // UDFs
  // val countTags = udf((tags: String) => {
  //   "&lt;".r.findAllMatchIn(tags).length
  // })
  // perferable to use sparksession
  val countTags = spark.udf.register("countTags",
    (tags: String) => {
      "&lt;".r.findAllMatchIn(tags).length
    }
  )

  val exp7 = itPostsDF.filter($"postTypeId" === 1).select($"tags", countTags($"tags") as "tagCnt")  //.show(10, false)
  // pure sql
  val exp7Alt = spark.sql(
    """
    SELECT tags, countTags(tags) as tagCnt FROM posts WHERE postTypeId = 1
    """
  )

  // missing values
  val cleanPosts = itPostsDF.na.drop()
  cleanPosts.count()

  itPostsDF.na.drop(Array("acceptedAnswerId"))

  itPostsDF.na.fill(Map("viewCount" -> 0))

  itPostsDF.na.replace(Array("id", "acceptedAnswerId"), Map(1177 -> 3000))

  // skipping DF to rdd conversion 
  // user-defined aggregate functions
  val smplDf = itPostsDF.where($"ownerUserId" >= 13 and $"ownerUserId" <= 15)
  val exp8 = smplDf.groupBy($"ownerUserId", $"tags", $"postTypeId").count
  // with sql
  val exp8Alt = spark.sql(
    """
    SELECT ownerUserId, tags, postTypeId, count(ownerUserId) as count
    FROM posts
    WHERE ownerUserId >= 13 AND ownerUserId <= 15
    GROUP BY ownerUserId, tags, postTypeId
    """)

  // rollup
  val exp9 = smplDf.rollup($"ownerUserId", $"tags", $"postTypeId").count
  val exp9Alt = spark.sql(
    """
    SELECT ownerUserId, tags, postTypeId, COUNT(ownerUserId) as count
    FROM posts
    WHERE ownerUserId >= 13 AND ownerUserId <= 15
    GROUP BY ROLLUP (ownerUserId, tags, postTypeId)
    """
  )

  val exp9Cube = spark.sql(
    """
    SELECT ownerUserId, tags, postTypeId, COUNT(ownerUserId) as count
    FROM posts
    WHERE ownerUserId >= 13 AND ownerUserId <= 15
    GROUP BY CUBE (ownerUserId, tags, postTypeId)
    """
  )
}
