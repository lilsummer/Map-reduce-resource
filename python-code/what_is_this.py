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
regex = re.compile(r':|#')
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
         # MRStep(mapper_init=self.chunk_init,
         #     mapper=self.mapper_get_words,
          #     reducer=self.first_reducer)
          MRStep(mapper_init=self.chunk_init,
              mapper=self.mapper_get_words,
                reducer=self.first_reducer),
              MRStep(reducer=self.second_reducer),
              MRStep(reducer=self.third_reducer)
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
                    title = doc.find('title').text
                    title = title.split('|')[0].split(']]')[0].encode('utf-8').lower()
                     #pid = doc.find('id/text').text
                    if text and not regex.search(title):
                        wikicode = mwparserfromhell.parse(text)
                        links = wikicode.filter_wikilinks()
                        links_new = set([link.encode('utf-8').lower() for link in links])
                        links_new_filter = filter(lambda i: not regex.search(i), links_new)
                        links_new_len = len(links_new_filter)
                        for link in links_new_filter:
                            match = parselink.search(link)
                            clean_link = match.group(0).split('[[')[1].split('|')[0].split(']]')[0]
                            yield (title, (clean_link, 1/float(links_new_len + 10), 'out'))
                            yield (clean_link, (title, 1/float(links_new_len + 10), 'in'))
                except:
                    pass
                 
                self.chunk = ''
                 
        if startStr in line:
            self.isPage = True
            self.chunk = line
   
   
   
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