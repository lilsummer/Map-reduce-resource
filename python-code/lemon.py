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
              reducer=self.first_reducer)
           # MRStep(mapper_init=self.second_mapper_init,
           #        mapper=self.second_mapper,
           #        mapper_final=self.second_mapper_final)
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
                        yield (title, clean_link)   
                self.chunk = ''
                
        if startStr in line:
            self.isPage = True
            self.chunk = line
    
    
    
    def first_reducer(self, title, links):
            g_list = []
            for l in links:
                if not l in g_list:
                    g_list.append(l)
            len_list = len(g_list)
            yield(title, (g_list,  1/float(len_list + 10)))
           
        
  
    def second_mapper(self, title, pair):
        g_list = pair[0]
        wt = pair[1]
        for g in g_list:
            yield(title, (g, wt))
            yield(g, (title, -wt))
            
    def second_reducer(self, title, list_of_link_weight_pair):
        income = []
        outgo = []
        for l in list_of_link_weight_pair:
            wt = l[1]
            lk = l[0]
            if wt > 0:
                
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    