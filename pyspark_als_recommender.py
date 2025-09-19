
"""
OOP refactor: MovieRecommender class for PySpark ALS pipeline.
Encapsulates data loading, training, evaluation, and recommendation logic.
"""
from pyspark.sql import SparkSession
from pyspark.ml.recommendation import ALS
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import col, explode

class MovieRecommender:
	def __init__(self, data_dir="data/ml-latest-small", model_path="als_model"):
		self.data_dir = data_dir
		self.model_path = model_path
		self.spark = SparkSession.builder.appName("MovieLensALS").getOrCreate()
		self.ratings = None
		self.movies = None
		self.model = None

	def load_data(self):
		"""Load ratings and movies data from CSV files."""
		self.ratings = self.spark.read.csv(f"{self.data_dir}/ratings.csv", header=True, inferSchema=True)
		self.movies = self.spark.read.csv(f"{self.data_dir}/movies.csv", header=True, inferSchema=True)
		# Cast columns to correct types
		self.ratings = self.ratings.select(col("userId").cast("integer"), col("movieId").cast("integer"), col("rating").cast("float"))

	def split_data(self):
		"""Split ratings into training and test sets."""
		return self.ratings.randomSplit([0.8, 0.2], seed=42)

	def train(self, training):
		"""Train ALS model on training data."""
		als = ALS(userCol="userId", itemCol="movieId", ratingCol="rating", coldStartStrategy="drop", nonnegative=True)
		self.model = als.fit(training)
		return self.model

	def save_model(self):
		"""Save the trained ALS model, overwriting if the path exists."""
		if self.model:
			self.model.write().overwrite().save(self.model_path)

	def evaluate(self, test):
		"""Evaluate the model using RMSE."""
		predictions = self.model.transform(test)
		evaluator = RegressionEvaluator(metricName="rmse", labelCol="rating", predictionCol="prediction")
		rmse = evaluator.evaluate(predictions)
		print(f"Test RMSE: {rmse:.3f}")
		return rmse

	def recommend_for_all_users(self, num_recs=10):
		"""Generate top-N recommendations for all users."""
		return self.model.recommendForAllUsers(num_recs)

	def show_sample_recommendations(self, userRecs):
		"""Show recommendations for a sample user, with movie titles."""
		sample_user = self.ratings.select("userId").distinct().limit(1).collect()[0][0]
		print(f"\nTop 10 recommendations for user {sample_user}:")
		userRecs.filter(col("userId") == sample_user).show(truncate=False)
		# Join with movie titles
		recs = userRecs.withColumn("rec", explode("recommendations")).select("userId", col("rec.movieId"), col("rec.rating"))
		recs = recs.join(self.movies, on="movieId").select("userId", "title", "rating")
		print("\nSample recommendations with movie titles:")
		recs.filter(col("userId") == sample_user).show(truncate=False)

	def stop(self):
		self.spark.stop()

if __name__ == "__main__":
	recommender = MovieRecommender()
	recommender.load_data()
	training, test = recommender.split_data()
	recommender.train(training)
	recommender.save_model()
	recommender.evaluate(test)
	userRecs = recommender.recommend_for_all_users(10)
	recommender.show_sample_recommendations(userRecs)
	recommender.stop()
