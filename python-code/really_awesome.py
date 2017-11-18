from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
import re
import heapq
import mwparserfromhell
import numpy as np
import math
import itertools

regex = re.compile(r'image:|wikt:|file:|talk:|help:|category:|template:')
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
         # MRStep(mapper_init=self.chunk_init,
         #     mapper=self.mapper_get_words,
          #     reducer=self.first_reducer)
          MRStep(mapper_init=self.mapper_init1,
              mapper=self.mapper1,
                reducer=self.first_reducer),
              MRStep(reducer=self.second_reducer),
              MRStep(reducer=self.third_reducer)
        ]


#WORD_RE = re.compile(r"\w|\s")
    def mapper_init1(self):
        self.s = []
        
    def mapper1(self, _, line):
        line = line.lower()
        self.s.append(line)
        if '<page>' in line:
            self.s = [line]
        elif '</page>' in line:
            try:
                doc = etree.fromstring(' '.join(self.s))
                from_title = doc.find('title').text
                if regex.search(from_tile) == None:
                    text_segment = doc.find('revision').find('text').text
                    wi = mwparserfromhell.parse(text_segment).filter_wikilinks()
                    linkto = set()
                    for item in wi:
                        to_title = item.title.__unicode__()
                        if ':' not in to_title and regex.search(to_tile)== None:
                            linkto.add(to_title)
                    weight = 1.0 / (len(linkto) + 10)
                    for to_title in linkto:
                        yield (from_title, (to_title, weight, 'out'))
                        yield (to_title, (from_title, weight, 'in'))
            except:
                pass
   
   
    def first_reducer(self, title, things):
        income=[]
        outgo=[]
        for t in things:
            link1 = t[0]
            wt1 = t[1]
            tag = t[2]
            if tag == 'in':
                  # yield (link1, wt1)
                income.append((link1,wt1))
            else:
                outgo.append((link1,wt1))
        for i in income:
            for j in outgo:
                if j[0]!=i[0]:
                     #yield((j[0], i[0]), (j[1]*i[1])
                    yield((min(j[0], i[0]), max(j[0],i[0])), j[1]*i[1])
               
    def second_reducer(self, headtail, wt):
        yield(None, (headtail, sum(wt)))
       
    def third_reducer(self, _, pair):
        h = []
        for (headtail, wt) in pair:
            if len(h) < 100:
                heapq.heappush(h, (wt, headtail))
                 
            elif h[0][0] < wt:
                heapq.heapreplace(h, (wt, headtail))
         
        for item in h:
            yield ((item[1][0], item[1][1]), item[0])
 
      


if __name__ == '__main__':
    MRMostUsedWord.run()