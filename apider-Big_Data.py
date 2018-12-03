from pyspark import SparkContext, SparkConf
import scrapy

#scrapy que lê a página e retorna um arquivo .json com as palavras
class MainSpider(scrapy.Spider):
    name ='main-spider'
    start_urls = ['http://quotes.toscrape.com/']

    def parse(self, response):
        self.log('{}'.format(response.url))
        texts = response.xpath('//span[@class="text"]/text()').extract()
        
        print(texts)

        for text in texts:
            yield {
                'text': text
            }
#aplicação spark que importa o arquivo .json          
conf = SparkConf().setAppName("WordCount")                          #cria o app Counter
sc = SparkContext.getOrCreate()                                     #instancia SparkContext
rdd = sc.textFile('<path-to-data>/bigdata.txt')                     #chama o arquivo .json do scrapy
filter_empty_lines = contentRDD.filter(lambda x: len(x) > 0)        #elmina linhas em branco
words = filter_empty_lines.flatMap(lambda x: x.split(' '))          #splita as palavras pelo espaço em branco

#map-reduce que conta as palavras do arquivo .json
wordcount = words.map(lambda x:(x,1)) \
.reduceByKey(lambda x, y: x + y) \
.map(lambda x: (x[1], x[0])).sortByKey(False)

for word in wordcount.collect():
    print(word)

wordcount.saveAsTextFile("/home/thiago/Counter")
