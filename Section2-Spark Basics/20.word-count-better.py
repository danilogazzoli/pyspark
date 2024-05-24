import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)
sc.setLogLevel("ERROR")

input = sc.textFile("19.Book.txt")
words = input.flatMap(normalizeWords)
print(words.take(0))
# mantém a chave (palavra) e faz a contagem
wordCounts = words.countByValue()
print(wordCounts)
# output: {'self': 111, 'employment': 75, 'building': 33, 'an': 178, 'internet': 26, 'business': 383, 'of': 970, 'one': 100, 'achieving': 1, 'financial': 17, 'and': 934, 'personal': 48, 'freedom': 41, 'through': 57, 'a': 1191, 'lifestyle': 44, 'technology': 11, 'by': 122, 'frank': 11, 'kane': 10, '': 772, 'copyright': 3, '2015': 4, 'all': 137, 'rights': 3, 'reserved': 2, 'worldwide': 4, 'contents': 1, 'disclaimer': 2, 'preface': 2, 'part': 33, 'i': 387, 'making': 25, 'the': 1292, 'big': 42, 'decision': 12, 'overcoming': 2, 'inertia': 2, 'fear': 3, 'failure': 3, 'career': 31, 'indoctrination': 5, 'carrot': 4, 'on': 428, 'stick': 6, 'ego': 3, 'protection': 7, 'your': 1420, 'employer': 44, 'as': 343, 'security': 8, 'blanket': 2, 'why': 25, 'it': 649, 's': 391, 'worth': 39, 'unlimited': 6, 'growth': 39, 'potential': 38, 'investing': 16, 'in': 616, 'yourself': 78, 'not': 203, 'someone': 62, 'else': 33, 'no': 76, 'dependencies': 6, 'commute': 14, 'to': 1828, 'live': 25, 'where': 53, 'you': 1878, 'want': 122, 'work': 144, 'when': 102, 'how': 163, 'is': 560, 'for': 537, 'flowchart': 4, 'should': 69, 'even': 104, 'consider': 26, 'having': 30, 'safety': 7, 'net': 13, 'planning': 16, 'health': 35, 'care': 24, 'assessment': 4, 'quiz': 4, 'ii': 2, 'happen': 13, 'designing': 4, 'fallacy': 2, 'introducing': 3, 'ideal': 3, 'case': 26, 'study': 4, 'sundog': 24, 'software': 60, 'other': 78, 'ideas': 27, 'key': 6, 'points': 5, 'evaluating': 4, 'idea': 60, 'writing': 15, ... }
'''
for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
'''