# write a python mr program to load movies data file from hdfs /user/training/movie/movies.csv and show the year where more movies are 
# released along with movies count.

from mrjob.job import MRJob

class movieYearNCount(MRJob):
    def mapper(self, _, line):
        if line[0] != 'm':
            mv = line.split(',')[1].strip('"')
            c = 0
            # yield(mv[-5:-1],1)
            for i,val in enumerate(mv):
                if val.isnumeric():
                    
                    c += 1
                    if c == 4:
                        yield(mv[i-3:i+1],1)
                        break
                else:
                    c = 0
    
    def reducer(self,key, val):
        yield(key,sum(val))

if __name__ == '__main__':
    movieYearNCount.run()

