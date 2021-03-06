#Let's look at some summary statistics on the number of unique links on a page to other Wikipedia articles. Return the number of articles (count), average number of links, standard deviation, and the 25%, median, and 75% quantiles.


from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
import re
import heapq
import mwparserfromhell
import numpy as np
import math

#WORD_RE = re.compile(r"\w|\s")
#PAGE_RE = re.compile('<page>')
parselink = re.compile(r'\[\[(.*?)(\]\]|\|)')

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
              reducer=self.first_reducer),
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
        self.Parse = False
 
    def mapper_get_words(self, _, line):
        startStr = '<page>'
        endStr = '</page>'
        if self.isPage:
            self.chunk += (' ' + line)
            if endStr in line:              
                self.isPage = False
                doc = etree.fromstring(self.chunk)
                text = doc.find('revision/text').text
                pid = doc.find('id/text').text
                if text:
                    wikicode = mwparserfromhell.parse(text)
                    links = wikicode.filter_wikilinks()
                    links_new = set([link.encode('utf-8') for link in links])
                    yield (_, len(links_new))
                        
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
            
    def first_reducer(self, _, sum_links):
        yield(_, sum(sum_links))
        
    def second_mapper_init(self):
            self.pages = 0
            self.links = 0
            
    def mapper_count(self, _, links):
            self.pages += 1
            self.links += links
    
    def mapper_final(self):
            yield(None, (self.pages, self.links))
    
            
    def reducer_init(self):
        self.p = 0
        self.l = 0
        self.mean = 0
            
    def reducer_cal(self, _, pairs):
            for p in pairs:
                self.p += pairs[0]
                self.l += pairs[1]
            self.mean = self.l/float(self.p)
            
    def reducer_final(self):
            yield(self.mean, self.p)
                
                
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    