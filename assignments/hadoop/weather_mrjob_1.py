from mrjob.job import MRJob
import calendar as cl
# class Weather(MRJob):

#     def mapper(self, _, line):
#         yield('temp_max',float(line.split()[5]))
#         yield('temp_min',float(line.split()[6]))
    
#     def reducer(self,key, temp):
#         yield(key,max(temp) if key == 'temp_max' else min(temp))

# if __name__ == '__main__':
#     Weather.run()


class Weather(MRJob):

    def mapper(self, _, line):
        yield(line.split()[1][4:6], (float(line.split()[5]), float(line.split()[6])))
        # yield('temp_min',float(line.split()[6]))
    
    def reducer(self,key, temp):
        mx = []
        mn = []
        for i in temp:
            mx.append(i[0])
            mn.append(i[1])
        mx_val = max(mx)
        mn_val = min(mn)
        yield(cl.month_name[int(key)],(mn_val,mx_val))# min([i[1] for i in temp]))#,min([i[1] for i in temp])] ) #int(temp.split(',')[0]))

if __name__ == '__main__':
    Weather.run()

