#I see.so british english rapper might be a easier job than Am english rapper..
#
# this is for calculating entropy in data-lang

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
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
        combiner=self.combiner_count_words,
        reducer=self.reducer_count_words),

            MRStep(reducer=self.reducer_get_sum),
            MRStep(reducer=self.reducer_final)
        ]
    def chunk_init(self):
        self.chunk = ''
        self.isPage = False
        #self.sum1 = 888888 # what ever the sum it is
        #self.sum2 = 0
        #self.sum3 = 0
  
# wikicode = mwparserfromhell.parse(text)
#    text = " ".join(" ".join(fragment.value.split())
#                    " ".join(" ".join(fragment.value.split())

    def mapper_get_words(self, _, line):
        startStr = '<page>'
        endStr = '</page>'
        if self.isPage:
            self.chunk += (' ' + line)
            if endStr in line:
                self.isPage = False
                doc = etree.fromstring(self.chunk)
                text = doc.find('revision/text').text
                if text:
                    wikicode = mwparserfromhell.parse(text)
                    text = " ".join(" ".join(fragment.value.split())
                                    for fragment in wikicode.filter_text())
              
                            
    #for i in range(0,len(text)):
    #gram1 = text[i]
    #if i+2 <= len(text):
        #gram2 = text[i:i+2]
                    
                    
                    for word in text:
                        yield (word.lower(),1)  
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
        
    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))
        
    def reducer_count_words(self, word, counts):
        entroA = sum(counts)
        entroB = math.log(entroA,2)
        entroC = entroA * entroB
        yield (None, entroC)
      
    ### you can add another reducer
    def reducer_get_sum(self, _, entros):
        yield (None, sum(entros))
        
    def reducer_final(self, _, sum_entros):
        for sum_entro in sum_entros:
            entroD = sum_entro/455764365  ## this is for data-lang
            final = math.log(455764365,2) - entroD
            yield (None, final)
if __name__ == '__main__':
    MRMostUsedWord.run()    