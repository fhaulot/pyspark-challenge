# MovieLens Recommender System with PySpark

This project is a movie recommendation system built with PySpark, using the MovieLens dataset and the Alternating Least Squares (ALS) collaborative filtering algorithm. It features a web interface with Streamlit.

## Features
- Download and prepare the MovieLens dataset automatically
- Train a collaborative filtering model (ALS) with PySpark
- Generate movie recommendations for users
- User-friendly web interface with Streamlit
- Containerized with Docker for easy deployment

## Requirements

Install dependencies with:
```bash
pip install -r requirements.txt
```

**requirements.txt:**
- pyspark
- pandas
- streamlit

## Usage

1. **Download the MovieLens data:**
	```bash
	python download_movielens.py
	```

2. **Train the ALS recommender and save the model:**
	```bash
	python pyspark_als_recommender.py
	```

3. **Launch the Streamlit web app:**
	```bash
	streamlit run streamlit_app.py
	```
	Then open [http://localhost:8501](http://localhost:8501) in your browser.


## Docker Usage

You can run the entire project in a Docker container for a fully isolated environment (no need for a local Python/Java install).

### Build the Docker image
```bash
docker build -t pyspark-challenge .
```

### Run the container (production mode)
```bash
docker run -p 8501:8501 pyspark-challenge
```

### Development mode: live code updates
To develop and test your code without rebuilding the image at every change, mount your project directory as a volume:
```bash
docker run -p 8501:8501 -v $(pwd):/app pyspark-challenge
```
This way, any code change on your machine is instantly reflected in the running container.

> **Note:** If you add or change dependencies in `requirements.txt`, you must rebuild the image.

## Project Structure

- `download_movielens.py` — Download and extract the MovieLens dataset
- `pyspark_als_recommender.py` — Train and evaluate the ALS model, save it for later use
- `streamlit_app.py` — Streamlit web app for user recommendations
- `requirements.txt` — Python dependencies
- `Dockerfile` — Containerization for deployment

## Data

The project uses the [MovieLens ml-latest-small](https://grouplens.org/datasets/movielens/) dataset (included via script).

## How it works

1. The dataset is downloaded and extracted.
2. The ALS model is trained on user ratings and saved.
3. The Streamlit app loads the model and allows users to get movie recommendations by entering their user ID.

## License

This project is for educational purposes.
