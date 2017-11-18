from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
import re
import heapq
import mwparserfromhell
import numpy as np
import math
from random import randint
import numpy as np
import random
#WORD_RE = re.compile(r"\w|\s")
#PAGE_RE = re.compile('<page>')
parselink = re.compile(r'\[\[(.*?)(\]\]|\|)')

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
               reducer_init=self.reducer_init,
               reducer=self.reducer_cal,
               reducer_final=self.reducer_final)
        ]
    def chunk_init(self):
        self.chunk = ''
        self.isPage = False
        self.pagetag = 0

 
    def mapper_get_words(self, _, line):
        startStr = '<page>'
        endStr = '</page>'
        if self.isPage:
            self.chunk += (' ' + line)
            if endStr in line:              
                self.isPage = False
                self.pagetag += 1
                try:
                    doc = etree.fromstring(self.chunk)              
                    text = doc.find('revision/text').text
                    if text:
                        wikicode = mwparserfromhell.parse(text)
                        links = wikicode.filter_wikilinks()
                        links_new = set([link.encode('utf-8') for link in links])
                        yield (_, (self.pagetag, len(links_new)))
                except:
                    pass
                
                self.chunk = ''
                
        if startStr in line:
            self.isPage = True
            self.chunk = line
                    
    def reducer_init(self):
        self.median = 0
        self.quantile25 = 0
        self.quantile75 = 0
        self.sd = 0
        self.mean = 0
        self.page = 0
        
    def reducer_cal(self, _, page_link_pair):
        new_links = []
        for i in page_link_pair:
            self.page+=1
            if random.random() < 0.1:
                new_links.append(i[1])
                
        ## calculating quantile and median here
        self.median = np.median(new_links)
        self.quantile25 = np.nanpercentile(new_links, 25)
        self.quantile75 = np.nanpercentile(new_links, 75)
        self.sd = np.std(new_links)
        self.mean = np.mean(new_links)
        
    
    def reducer_final(self):
            yield(('median','25','75','sd','mean','page'),(self.median, self.quantile25, self.quantile75, self.sd, self.mean, self.page))
                
                
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    