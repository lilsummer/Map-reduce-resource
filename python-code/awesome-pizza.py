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
            mapper=self.mapper_get_words),
        #combiner=self.combiner_count_words,
        #reducer=self.reducer_count_words)
        MRStep(mapper_init=self.second_mapper_init,
               mapper=self.mapper_count,
               mapper_final=self.mapper_final,
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
                doc = etree.fromstring(self.chunk)
                text = doc.find('revision/text').text
                #pid = doc.find('id/text').text
                if text:
                    wikicode = mwparserfromhell.parse(text)
                    links = wikicode.filter_wikilinks()
                    links_new = set([link.encode('utf-8') for link in links])
                    yield (self.pagetag, len(links_new))
                        
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
            
        
    def second_mapper_init(self):
            #self.pages = 0
            self.links = 0
            self.decision = 0
            
    def mapper_count(self, pid, links):
            #self.pages += 1
            self.links = links
            self.decision = randint(0, 188418)
    
    def mapper_final(self):
            yield(1, (self.decision, self.links))
    
            
    def reducer_init(self):
        self.median = 0
        self.quantile25 = 0
        self.quantile75 = 0
        self.sd = 0
        self.threshould = 500000
        self.judge = 3
            
    def reducer_cal(self, _, pairs):
        new_links = []
        for i in pairs:
            decision = i[0]
            links = i[1]
            if decision < 80000:
                new_links.append(links)
                
        ## calculating quantile and median here
        self.median = np.median(new_links)
        self.quantile25 = np.nanpercentile(new_links, 25)
        self.quantile75 = np.nanpercentile(new_links, 75)
        self.sd = np.std(new_links)
    
    def reducer_final(self):
            yield(('median','25','75','sd'),(self.median, self.quantile25, self.quantile75, self.sd))
                
                
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    