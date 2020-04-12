import requests
from bs4 import BeautifulSoup
import pandas
import scrapy
from scrapy.crawler import CrawlerProcess
import datetime
from datetime import date
import csv

# 1º MÈTODE AMB BEAUTIFULSOUP:

# descarreguem el lloc web d'interès:
page = requests.get('https://www.finanzas.com/divisas/')
soup = BeautifulSoup(page.content, 'html.parser')
# fem l'extracció de dades sobre tipus de canvi entre divises:
tipus_canvi = soup.find(class_ = 'currencies_layer table-container')
taula = tipus_canvi.find('tbody')
monedas = taula.find_all('tr')

# Creem les llistes buides.
llista_moneda = []
llista_moneda1 = []
llista_max = []
llista_min = []

# Afegim les dades a les llistes:
for moneda in monedas:
    llista_moneda.append(moneda.find(class_ = 'title').get_text())
    tds = moneda.find_all('td')
    llista_moneda1.append(tds[1].get_text())
    llista_max.append(tds[4].get_text())
    llista_min.append(tds[5].get_text())

# Creem el csv amb el mètode utilitzant BeautifulSoup:
tdc_divises = pandas.DataFrame(
    {'Divises': llista_moneda,
     'Tipus de canvi actual': llista_moneda1,
     'TdC-màxim diari': llista_max,
     'TdC-mínim diari': llista_min
     })

# farem servir el csv del 2º mètode per a la entrega de la pràctica.
# Generem el csv amb el mètode BeautifulSoup:
#tdc_divises.to_csv('TdC-BeautifulSoup')



# 2º MÈTODE AMB UNA SPIDER DE SCRAPY:

# Creem el csv amb les capçaleres (aquesta part del codi només ha d'executar-se el primer dia.
with open('TdC-Divises-per-dia', 'w', newline='') as csvfile:
    writer = csv.writer(csvfile)
    writer.writerow(['Divises', 'Tipus de canvi', 'Tipus màxim', 'Tipus mínim', 'Data'])
    

# Creem la spider:
class SpiderTdc(scrapy.Spider):
    name = "spider_tdc"
    custom_settings = { 'CONCURRENT_REQUESTS': '1' }
    
    # mètode start_requests on indiquem la primera pàgina web (lloc web objectiu):
    def start_requests(self):
        urls = ['https://www.finanzas.com/divisas/']
        for url in urls:
            # passem el url al següent mètode parse:
            yield scrapy.Request(url = url, callback = self.parse)

    def parse(self, response):
        # busquem els url d'on extraurem les dades i els guardem a una llista:
        links = response.css('tbody td.title > a::attr(href)').extract()

        for link in links:
            # passem tots els url de la llista links al següent mètode parse2:
            yield response.follow(url = link, callback = self.parse2)

    def parse2(self, response):
        # en els url recollits al mètode parse fem l'extracció de dades utilitzant CSS Locator i Xpath:
        tdc = response.css('div#cotizaciones div.values-right > span::text').extract()
        nom = response.xpath('//div[@class="m-border main-ibex"]/h1/a/text()').extract()
        maxim = response.xpath('//table[@class="m-ranking m-top"]/tbody/tr[1]/td[5]/text()').extract()
        minim = response.xpath('//table[@class="m-ranking m-top"]/tbody/tr[1]/td[6]/text()').extract()
        # afegim les dades extretes mitjançant web scraping a la llista següent:
        llista_tdc.append(nom)
        llista_tdc.append(tdc)
        llista_tdc.append(maxim)
        llista_tdc.append(minim)
        llista_tdc.append(date.today())

        # Afegim les dades del dia al csv:
        with open('TdC-Divises-per-dia', 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(llista_tdc)

        # borrem les dades de la llista per a la següent iteració de la spider:
        llista_tdc.clear()


# Inicialitzem la llista utilitzada a la spider:
llista_tdc = []

# processem i executem la aranya:
process = CrawlerProcess()
process.crawl(SpiderTdc)
process.start()

