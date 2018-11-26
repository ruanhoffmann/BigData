import scrapy

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