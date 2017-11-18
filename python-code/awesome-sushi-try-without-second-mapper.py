from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
import re
import heapq
import mwparserfromhell
import numpy as np
import math
import itertools

#WORD_RE = re.compile(r"\w|\s")
#PAGE_RE = re.compile('<page>')
parselink = re.compile(r'\[\[(.*?)(\]\]|\|)')
regex = re.compile(r':')

class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
              reducer=self.reducer_simple),
            MRStep(mapper_init=self.second_mapper_init,
                   mapper=self.second_mapper)
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
                title = doc.find('title').text
                #pid = doc.find('id/text').text
                if text and not regex.search(title):
                    wikicode = mwparserfromhell.parse(text)
                    links = wikicode.filter_wikilinks()
                        
                    links_new = set([link.encode('utf-8') for link in links])
                    links_new_filter = filter(lambda i: not regex.search(i), links_new)
                    links_new_len = len(links_new_filter)
                    
                    for link in links_new_filter:
                        match = parselink.search(link)
                        clean_link = match.group(0).split('[[')[1].split('|')[0].split(']]')[0]
                        yield (title, (clean_link, 1/float(links_new_len + 10)))
                        yield (clean_link, (title, -(1/float(links_new_len + 10))))        
                self.chunk = ''
                
        if startStr in line:
            self.isPage = True
            self.chunk = line
            
    
        
    def reducer_simple(self, title, link_weight_list):
        yield(None, link_weight_list)
    
    
    def second_mapper_init(self):
        self.weight = 1
        self.head = ''
        self.tail = ''
        
    def second_mapper(self, _, link_weight_list):
        new_pair = list(itertools.combinations(link_weight_list, 2))
        for p in new_pair:
            link1 = p[0][0]
            wt1 = p[0][1]
            link2 = p[1][0]
            wt2 = p[1][1]
            yield(link1, wt1)
            yield(link2, wt2)
    
    def second_reduer(self, title, link_weight_list):
        if len(link_weight_list) > 1:
            for element in link_weight_list:
                new_pair = list(itertools.combinations(link_weight_list, 2))
                for p in new_pair:
                    yield(None, p)
                
   
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    