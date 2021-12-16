import scala.math._;
import org.apache.spark.sql._;
import org.apache.spark.sql.functions._;
import org.apache.spark.sql.{Column => col};
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.types.{
  StringType,
  IntegerType,
  StructType,
  StructField
};

var videoData = spark.read.option("header", "true").option("inferSchema", "true").format("csv").load("EdurekaSparkProjects/USvideos.csv");

var categorySchema = StructType(Array(
	StructField("category_id", IntegerType, true),
	StructField("category_name", StringType, true)
));

var categoryData = spark.read.option("inferSchema", "true").schema(categorySchema).format("csv").load("EdurekaSparkProjects/USvideos.csv");

videoData = videoData.withColumn("totalInteraction", col("views") + col("likes") + col("dislikes"));
videoData = videoData.withColumn("trendingDateFormatted", to_date(col("trending_date"), "yy.dd.MM"));
videoData = videoData.orderBy(desc("trendingDateFormatted"));
videoData = videoData.withColumn("year", year(col("trendingDateFormatted")));
videoData = videoData.withColumn("month", month(col("trendingDateFormatted")));
videoData = videoData.withColumn("likeDislikeRatio", abs(col("likes") - col("dislikes")) / (col("likes") + col("dislikes")));
videoData = videoData.withColumn("unixTrending", unix_timestamp(col("trendingDateFormatted")));

// 1 Top 3 videos for which user interaction is highest (views + likes + dislikes)
var overallTop3 = videoData.groupBy("video_id").max("totalInteraction").orderBy(desc("max(totalInteraction)")).take(3);

// 2 Top 3 videos for which user interaction is lowest (views + likes + dislikes)
var overallBottom3 = videoData.groupBy("video_id").max("totalInteraction").orderBy(asc("max(totalInteraction)")).take(3);

// 3 Top 3 videos in each category in each year
// BY number of views
var top3views = Window.partitionBy("category_id", "year").orderBy(desc("views"));
var result = videoData.withColumn("row", row_number().over(top3views)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("views");

// BY number of comments
var top3comments = Window.partitionBy("category_id", "year").orderBy(desc("comment_count"));
var result = videoData.withColumn("row", row_number().over(top3comments)).filter(col("row") <= 3);
result  = result.groupBy("video_id").max("comment_count");

// BY number of likes
var top3likes = Window.partitionBy("category_id", "year").orderBy(desc("likes"));
var result = videoData.withColumn("row", row_number().over(top3likes)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("likes");

// Highest user interaction
var top3interaction = Window.partitionBy("category_id", "year").orderBy(desc("totalInteraction"));
var result = videoData.withColumn("row", row_number().over(top3interaction)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("totalInteraction");

// 4 Top 3 videos in each month (Likes or Dislikes ratio is highest)
var top3likedislikeratio = Window.partitionBy("month").orderBy(desc("likeDislikeRatio"));
var result = videoData.withColumn("row", row_number().over(top3likedislikeratio)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("likeDislikeRatio");

// 5 Top 3 videos in each category in each month
// BY number of comments
var top3comments = Window.partitionBy("category_id", "month").orderBy(desc("comment_count"));
var result = videoData.withColumn("row", row_number().over(top3comments)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("comment_count");

// BY number of likes
var top3likes = Window.partitionBy("category_id", "month").orderBy(desc("likes"));
var result = videoData.withColumn("row", row_number().over(top3likes)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("likes");

// BY number of dislikes
var top3dislikes = Window.partitionBy("category_id", "month").orderBy(desc("dislikes"));
var result = videoData.withColumn("row", row_number().over(top3dislikes)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("dislikes");

// 6 Top 3 channels
// BY number of total views
var top3views = Window.partitionBy("channel_title").orderBy(desc("views"));
var result = videoData.withColumn("row", row_number().over(top3views)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("views").sum("max(views)");

// Likes or Dislikes ratio is highest
var top3ratio = Window.partitionBy("channel_title").orderBy(desc("likeDislikeRatio"));
var result = videoData.withColumn("row", row_number().over(top3views)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("likeDislikeRatio").sum("max(likeDislikeRatio)");

// BY number of total comments
var top3ratio = Window.partitionBy("channel_title").orderBy(desc("comment_count"));
var result = videoData.withColumn("row", row_number().over(top3views)).filter(col("row") <= 3);
result = result.groupBy("video_id").max("comment_count").sum("max(comment_count)");

// use currentTimestamp!?!?!
// unixTimestamp
// 7 Top 3 videos using this formula (Views on most recent date / (Recent Date - Publish Date))
var resultDF = videoData.withColumn("formula", col("views") / (col("unixTrending") - unix_timestamp(col("publish_time")))).orderBy(desc("formula"));

// 8 Calculate any 3 videos which got atleast 5 comments on every 1000 views
var resultDF = videoData.withColumn("commentViewRatio", col("views") / col("comment_count")).filter(col("commentViewRatio") >= 0.005).orderBy(desc("commentViewRatio"));

// 9 Calculate any 3 videos which got atleast 4 likes on every 100 views
var resultDF = videoData.withColumn("likeViewRatio", col("views") / col("likes")).filter(col("likeViewRatio") >= 0.04).orderBy(desc("likeViewRatio"));

// 10 Number of videos published in each category
var resultDF = videoData.groupBy("category_id", "video_id").count().orderBy("count"); 
resultDF = resultDF.groupBy("category_id").count();