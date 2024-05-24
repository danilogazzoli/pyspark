from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")


input = sc.textFile("19.Book.txt")
words = input.flatMap(lambda x: x.split())
print(words.take(10))
#output: ['Self-Employment:', 'Building', 'an', 'Internet', 'Business', 'of', 'One', 'Achieving', 'Financial', 'and']

#conta as palavras
wordCounts = words.countByValue()
print(wordCounts)
#output: defaultdict {'Self-Employment:': 1, 'Building': 5, 'an': 172, 'Internet': 13, 'Business': 19, 'of': 941, 'One': 12, 'Achieving': 1, 'Financial': 3, 'and': 901, 'Personal': 3, 'Freedom': 7, 'through': 55, 'a': 1148, 'Lifestyle': 5, 'Technology': 2, 'By': 9, 'Frank': 10, 'Kane': 7, 'Copyright': 1, '�': 174, '2015': 3, 'Kane.': 1, 'All': 13, 'rights': 3, 'reserved': 2, 'worldwide.': 2, 'CONTENTS': 1, 'Disclaimer': 1, 'Preface': 1, 'Part': 2, 'I:': 2, 'Making': 5, 'the': 1176, 'Big': 1, 'Decision': 1, 'Overcoming': 1, 'Inertia': 1, 'Fear': 1, 'Failure': 1, 'Career': 1, 'Indoctrination': 2, 'The': 88, 'Carrot': 1, 'on': 399, 'Stick': 2, 'Ego': 1, 'Protection': 1, 'Your': 62, 'Employer': 2, 'as': 297, 'Security': 2, 'Blanket': 1, 'Why': 3, ... }

for word, count in wordCounts.items():
    cleanWord = word.encode('iso-8859', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
