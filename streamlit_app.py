"""
Streamlit web app for MovieLens PySpark Recommender System (OOP version).

Features:
- Lets the user select 5 favorite movies from the MovieLens dataset.
- Finds the 5 users in the dataset with the most movies in common with the selection.
- Aggregates and averages the recommendations from these users using a trained PySpark ALS model.
- Displays the top 10 recommended movies with their average predicted score.

Requirements:
- Trained ALS model saved as 'als_model'.
- MovieLens ratings and movies data in 'data/ml-latest-small/'.
- All dependencies installed (see requirements.txt).
"""

import streamlit as st
import pandas as pd
from pyspark.sql.functions import col, explode
from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession

# Import the MovieRecommender class from pyspark_als_recommender.py
from pyspark_als_recommender import MovieRecommender


st.title("Movie Recommender System")

# Use the MovieRecommender class for Spark session, model, and movies
recommender = MovieRecommender()
recommender.load_data()

# Load ALS model (must be saved previously)
try:
    recommender.model = ALSModel.load(recommender.model_path)
    st.success("Model loaded.")
except Exception as e:
    st.error(f"Model not found. Please train and save the ALS model first. Error: {e}")
    recommender.stop()
    st.stop()

# Get list of movie titles for selection
movie_titles = recommender.movies.select("title").toPandas()["title"].tolist()

st.write("Select 5 of your favorite movies:")
favorite_movies = st.multiselect("Choose movies:", movie_titles, default=movie_titles[:5], max_selections=5)


if len(favorite_movies) == 5:
    # Get movieIds for selected titles
    selected_movies_df = recommender.movies.filter(col("title").isin(favorite_movies)).select("movieId").toPandas()
    selected_movie_ids = set(selected_movies_df["movieId"].tolist())

    # Find users with the most movies in common
    ratings_pd = recommender.ratings.toPandas()
    user_groups = ratings_pd.groupby("userId")["movieId"].apply(set)
    overlap = user_groups.apply(lambda s: len(selected_movie_ids & s))
    top_users = overlap.sort_values(ascending=False).head(5)
    top_user_ids = top_users[top_users > 0].index.tolist()

    if not top_user_ids:
        st.warning("No user rated your favorite movies, please try other movies.")
    else:
        st.info(f"Recommendations based on the {len(top_user_ids)} closest users.")

        # Get recommendations for each user 
        all_recs = []
        for uid in top_user_ids:
            user_df = recommender.spark.createDataFrame([(int(uid),)], ["userId"])
            recs = recommender.model.recommendForUserSubset(user_df, 10)
            recs = recs.withColumn("rec", explode("recommendations")).select(col("rec.movieId").alias("movieId"), col("rec.rating").alias("score"))
            all_recs.append(recs.toPandas())

        # Merge and average the scores
        if all_recs:
            import pandas as pd
            merged = pd.concat(all_recs)
            merged = merged.groupby("movieId").agg({"score": "mean"}).reset_index()
            merged = merged.sort_values("score", ascending=False).head(10)
            # Adding titles
            movie_titles_df = recommender.movies.select("movieId", "title").toPandas()
            merged = merged.merge(movie_titles_df, on="movieId", how="left")
            st.subheader("Top 10 personalized recommendations (average of closest users)")
            st.dataframe(merged[["title", "score"]].rename(columns={"title": "Movie", "score": "Average Score"}))

recommender.stop()

