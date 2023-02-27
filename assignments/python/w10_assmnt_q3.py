# mr job: find top 10 most frequently occuring words in a given 
# text file

from mrjob.job import MRJob

class WordCount(MRJob):
    def mapper(self, _, line):
        words = line.split()
        for i in words:
            yield(i,1)
            
    
    def reducer(self,key, val):
        yield(key,sum(val))
    
if __name__ == '__main__':
    WordCount.run()