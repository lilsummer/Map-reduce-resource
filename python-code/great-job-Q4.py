#I see.so british english rapper might be a easier job than Am english rapper..


from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
#import elementtree.ElementTree as etree
import re
import heapq
import mwparserfromhell

#WORD_RE = re.compile(r"\w|\s")
#PAGE_RE = re.compile('<page>')
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
        combiner=self.combiner_count_words,
        reducer=self.reducer_count_words)
        ]
    def chunk_init(self):
        self.chunk = ''
        self.isPage = False
        self.sum1 = 0
        self.sum2 = 0
        self.sum3 = 0
  
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
                    for word in text:
                        yield (1,1)  ## change this to 0.5
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
        
    def combiner_count_words(self, _, counts):
        # optimization: sum the words we've seen so far
        yield (1, sum(counts))
        
    def reducer_count_words(self, _, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield (None, (sum(counts), _))
        # discard the key; it is just None
        
        
if __name__ == '__main__':
    MRMostUsedWord.run()    