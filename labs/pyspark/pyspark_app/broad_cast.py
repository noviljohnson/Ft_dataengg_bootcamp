from pyspark import SparkContext 
sc = SparkContext("local", "Broadcast app") 
# words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"]) 
# data = words_new.value 
# print("Words new : ",words_new._value)
# for i in words_new.value:
#     print(i)
# print("Stored data -> %s" % (data)) 
# elem = words_new.value[2] 
# print("Printing a particular element in RDD -> %s" % (elem))


trs = sc.textFile('/mnt/c/Users/miles.MILE-BL-4766-LA.000/Documents/FT/Ft_Bigdata/Ft_dataengg_bootcamp/labs/datasets/transactions_t.txt')

# countries = sc.broadcast([('IN',"INDIA"),("US","AMERICA"),("NZ","NEWZEALAND"),("CH","CHAINA"),('AUS','AUSTRALIA'),('JP',"JAPAN")])

countries = sc.broadcast({'IN':"INDIA","US":"AMERICA","NZ":"NEWZEALAND","CH":"CHAINA",'AUS':'AUSTRALIA','JP':"JAPAN"})

output = trs.flatMap(lambda x: [x.split(',')]).map(lambda x: ((x[-1],x[1]),1)).reduceByKey(lambda a,b: a+b).collect()
output.sort()
for i,val in output:
    print(countries.value[i[0]],i[1],val, sep=' ')