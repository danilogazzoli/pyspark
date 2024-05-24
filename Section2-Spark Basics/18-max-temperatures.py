from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperatures")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("17-1800.csv")
parsedLines = lines.map(parseLine)
# filtra as linhas que contenham a string TMAX
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
#Saída:
#[('ITE00100554', 'TMAX', 18.5), ('EZE00100082', 'TMAX', 16.52), ('ITE00100554', 'TMAX', 21.2), ('EZE00100082', 'TMAX', 24.08), ('ITE00100554', 'TMAX', 27.86), ('EZE00100082', 'TMAX', 30.2), ('ITE00100554', 'TMAX', 32.0), ('EZE00100082', 'TMAX', 22.1), ('ITE00100554', 'TMAX', 33.8), ('EZE00100082', 'TMAX', 24.8)]
print(maxTemps.take(10))

#pega somente o primeiro e o último campo (estação climática e temperatura)
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
stationTemps.collect()
print(stationTemps.take(10))
#Saída: 
#[('ITE00100554', 18.5), ('EZE00100082', 16.52), ('ITE00100554', 21.2), ('EZE00100082', 24.08), ('ITE00100554', 27.86), ('EZE00100082', 30.2), ('ITE00100554', 32.0), ('EZE00100082', 22.1), ('ITE00100554', 33.8), ('EZE00100082', 24.8)]

#mantem a chave (estação climática) porém retorna o maior valor.
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
