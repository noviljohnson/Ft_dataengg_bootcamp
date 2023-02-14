from mrjob.job import MRJob

class countRatings(MRJob):

    def mapper(self, key, line):
        if line.split(',')[2][0] != 'r':
            yield(line.split(',')[2],1)
    
    def reducer(self, rating, count):
        yield(float(rating), sum(count))

if __name__ == '__main__':
    countRatings.run()
