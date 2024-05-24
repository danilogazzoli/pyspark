from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SpendByCustomerSorted")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def extractCustomerPricePairs(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("23.customer-orders.csv")
mappedInput = input.map(extractCustomerPricePairs)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
print(totalByCustomer.take(10))
print('*'*10)
#output: [(44, 4756.8899999999985), (35, 5155.419999999999), (2, 5994.59), (47, 4316.299999999999), (29, 5032.529999999999), (91, 4642.259999999999), (70, 5368.249999999999), (85, 5503.43), (53, 4945.299999999999), (14, 4735.030000000001)]


#Changed for Python 3 compatibility:
#flipped = totalByCustomer.map(lambda (x,y):(y,x))
#fez a troca do id pelo consumo total por cliente
flipped = totalByCustomer.map(lambda x: (x[1], x[0]))
print(flipped.take(10))
#output: [(4756.8899999999985, 44), (5155.419999999999, 35), (5994.59, 2), (4316.299999999999, 47), (5032.529999999999, 29), (4642.259999999999, 91), (5368.249999999999, 70), (5503.43, 85), (4945.299999999999, 53), (4735.030000000001, 14)]

#considera o total consumido como chave e faz o sort descending
totalByCustomerSorted = flipped.sortByKey(False)

results = totalByCustomerSorted.collect()
for result in results:
    print(result)
