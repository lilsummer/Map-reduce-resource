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
            mapper=self.mapper_get_words)
        #combiner=self.combiner_count_words,
        #reducer=self.reducer_count_words)
      
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
                if text:
                    wikicode = mwparserfromhell.parse(text)
                    links = wikicode.filter_wikilinks()
                        
                    links_new = set([link.encode('utf-8') for link in links])
                    for link in links_new:
                        match = parselink.search(link)
                        clean_link = match.group(0).split('[[')[1].split('|')[0].split(']]')[0]
                        yield (title, clean_link)
                        
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
            
    
        
    def reducer_simple(self, title, link):
        for i in range(0,100):
            yield(title, link)
   
       
    
if __name__ == '__main__':
    MRMostUsedWord.run()    