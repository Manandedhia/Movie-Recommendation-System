from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt

from itertools import combinations

class moviess(MRJob):

    def configure_args(self):
        super(moviess, self).configure_args()
        self.add_file_arg('--name', help='Path to u.item')

    def load_movie_names(self):
        self.movieNames = {}

        with open("movies.csv", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split(',')
                self.movieNames[fields[0]] = fields[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_parse_input,
                    reducer=self.reducer_ratings_by_user),
            MRStep(mapper=self.mapper_create_item_pairs,
                    reducer=self.reducer_compute_similarity),
            MRStep(mapper=self.mapper_sort_similarities,
                    mapper_init=self.load_movie_names,
                    reducer=self.reducer_output_similarities)]

    def mapper_parse_input(self, key, line):
        (userID, movieID, rating, timestamp) = line.split(',')
        yield  userID, (movieID, rating)

    def reducer_ratings_by_user(self, user_id, itemRatings):

        ratings = []
        for movieID, rating in itemRatings:
            ratings.append((movieID, rating))

        yield user_id, ratings

    def mapper_create_item_pairs(self, user_id, itemRatings):
        
        for itemRating1, itemRating2 in combinations(itemRatings, 2):
            movieID1 = itemRating1[0]
            rating1 = itemRating1[1]
            movieID2 = itemRating2[0]
            rating2 = itemRating2[1]

            yield (movieID1, movieID2), (float(rating1), float(rating2))
            yield (movieID2, movieID1), (float(rating2), float(rating1))


    def cosine_similarity(self, ratingPairs):
        
        numPairs = 0
        sum_xx = sum_yy = sum_xy = 0
        for ratingX, ratingY in ratingPairs:
            sum_xx += ratingX * ratingX
            sum_yy += ratingY * ratingY
            sum_xy += ratingX * ratingY
            numPairs += 1

        numerator = sum_xy
        denominator = sqrt(sum_xx) * sqrt(sum_yy)

        score = 0
        if (denominator):
            score = (numerator / (float(denominator)))

        return (score, numPairs)

    def reducer_compute_similarity(self, moviePair, ratingPairs):
        

        score, numPairs = self.cosine_similarity(ratingPairs)

        
        if (numPairs > 14 and score > 0.97):
            yield moviePair, (score, numPairs)

    def mapper_sort_similarities(self, moviePair, scores):
        
        score, n = scores
        movie1, movie2 = moviePair

        yield (self.movieNames[movie1], score), \
            (self.movieNames[movie2], n)

    def reducer_output_similarities(self, movieScore, similarN):
        
        movie1, score = movieScore
        for movie2, n in similarN:
            yield movie1, (movie2, score, n)


if __name__ == '__main__':
    moviess.run()