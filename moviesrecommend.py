from mrjob.job import MRJob
from mrjob.step import MRStep
from math import sqrt
from itertools import combinations

class moviesrecommend(MRJob):
    
    def configure_args(self):
        super(moviesrecommend,self).configure_args()
        self.add_file_arg('--name', help ="Movie names file")
        
    def load_movie_names(self):
        self.movieNames = {}

        with open("movies.csv", encoding='ascii', errors='ignore') as f:
            for line in f:
                fields = line.split(',')
                self.movieNames[int(fields[0])] = fields[1]
    
    def steps(self):
        return[
        MRStep(mapper=self.mapper_init,reducer=self.reducer_userrating),
        MRStep(mapper=self.mapper_combinations, reducer=self.reducer_pairs),
        MRStep(mapper=self.mapper_sortsimilar, mapper_init = self.load_movie_names, reducer=self.reducer_similaroutput)]
    
    def mapper_init(self,_,line):
        (userid,movieid,rating,timestamp) = line.split(',')
        yield userid,(movieid,rating)
        
    def reducer_userrating(self,userid,ratingpairs):
        ratings=[]
        for movie_id,movierating in ratingpairs:
            ratings.append((movie_id,movierating))
            yield userid, ratings
    
    def mapper_combinations(self,userid,ratings):
        for movierating1,movierating2 in combinations(ratings,2):
            
            movieid1 = movierating1[0]
            rating1 = movierating1[1]
            movieid2 = movierating2[0]
            rating2 = movierating2[1]
            yield (movieid1,movieid2), (rating1,rating2)
            yield (movieid2,movieid1), (rating2,rating1)
    
    
    def cosine_similarity(self,ratingpair):
        num_pairs = 0
        for ratingx,ratingy in ratingpair:
            x= x+ratingx*ratingx
            y = y+ratingy*ratingy
            xy= xy+ratingx*ratingy
            num_pairs = num_pairs+1
        
        score = 0
        numerator = xy
        denominator = sqrt(float(x))*sqrt(float(y))
        score = float(numerator/denominator)
        
        
    def reducer_pairs(self,moviepair,ratingpair):
        score,numpair = self.cosine_similairty(ratingpair)
        
        if(score > 0.95 and num_pairs > 11):
                yield moviepair,(score,num_pairs)
                
    def mapper_sortsimilar(self,moviepair, scores):
        score,n = scores
        movie1,movie2 = moviepair
        yield (self.movieNames[int(movie1)], score), (self.movieNames[int(movie2)], n)
            
    def reducer_similaroutput(self,movie,similarmovie):
        movie1, score = movie
        for movie2, n in similarmovie:
            yield movie1,(movie2, score, n)
       
if __name__ == '__main__':
    moviesrecommend.run()
    