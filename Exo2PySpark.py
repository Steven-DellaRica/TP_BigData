from pyspark import SparkContext
import pyspark

print(pyspark.__version__)

# Initialiser SparkContext
sc = SparkContext(appName="MovieLensAnalysisRDD")

# Charger les données en RDD
movies_rdd = sc.textFile("file:///root/ml-latest-small/movies.csv")
ratings_rdd = sc.textFile("file:///root/ml-latest-small/ratings.csv")
tags_rdd = sc.textFile("file:///root/ml-latest-small/tags.csv")

# Ignorer les en-têtes
movies_header = movies_rdd.first()
ratings_header = ratings_rdd.first()
tags_header = tags_rdd.first()

movies_rdd = movies_rdd.filter(lambda line: line != movies_header)
ratings_rdd = ratings_rdd.filter(lambda line: line != ratings_header)
tags_rdd = tags_rdd.filter(lambda line: line != tags_header)

# Transformation des RDDs
# Films: (movieId, title, genres)
movies_rdd = movies_rdd.map(lambda line: line.split(",")).map(lambda fields: (int(fields[0]), fields[1], fields[2]))

# Notes: (movieId, rating)
ratings_rdd = ratings_rdd.map(lambda line: line.split(",")).map(lambda fields: (int(fields[1]), float(fields[2])))

# Calculer la variance des notes pour chaque film
def calculate_variance(ratings):
    count = len(ratings)
    if count == 0:
        return 0.0
    mean = sum(ratings) / count
    variance = sum((x - mean) ** 2 for x in ratings) / count
    return variance

ratings_ratings = ratings_rdd.groupByKey().mapValues(list)
movie_variance = ratings_ratings.mapValues(calculate_variance)
movie_variance = movie_variance.filter(lambda x: x[1] > 0)  # Filtrer les films avec variance > 0

# Joindre les titres avec la variance
movies_with_variance = movies_rdd.map(lambda x: (x[0], (x[1], x[2]))) \
    .join(movie_variance) \
    .map(lambda x: (x[1][0][0], (x[1][0][1], x[1][1])))  # (title, (genre, variance))

# Trouver le TOP 10 des films avec la plus grande variance
top_10_max_variance = movies_with_variance.distinct() \
    .sortBy(lambda x: x[1][1], ascending=False) \
    .take(10)  # Obtenir les 10 films avec la plus grande variance

# Trouver le TOP 10 des films avec la plus petite variance
top_10_min_variance = movies_with_variance.distinct() \
    .sortBy(lambda x: x[1][1], ascending=True) \
    .take(10)  # Obtenir les 10 films avec la plus petite variance

# Afficher les résultats
print("Top 10 des films avec la plus grande variance des notes :")
for title, (genre, variance) in top_10_max_variance:
    print(f"Film: {title}, Genre: {genre}, Variance: {variance}")

print("\nTop 10 des films avec la plus petite variance des notes :")
for title, (genre, variance) in top_10_min_variance:
    print(f"Film: {title}, Genre: {genre}, Variance: {variance}")

# Arrêter SparkContext
sc.stop()