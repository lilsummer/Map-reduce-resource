from mrjob.job import MRJob
from mrjob.step import MRStep
from lxml import etree
#import elementtree.ElementTree as etree
import re
import heapq

WORD_RE = re.compile(r"[\w']+")
#PAGE_RE = re.compile('<page>')
class MRMostUsedWord(MRJob):
    def steps(self):
        return [
        MRStep(mapper_init=self.chunk_init,
            mapper=self.mapper_get_words,
        combiner=self.combiner_count_words,
        reducer=self.reducer_count_words),
        MRStep(reducer=self.reducer_find_max_word)
        ]
    def chunk_init(self):
        self.chunk = ''
        self.isPage = False
        
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
                    for word in WORD_RE.findall(text):
                        yield (word.lower(), 1)
                self.chunk = ''
        if startStr in line:
            self.isPage = True
            self.chunk = line
        
    def combiner_count_words(self, word, counts):
        # optimization: sum the words we've seen so far
        yield (word, sum(counts))
        
    def reducer_count_words(self, word, counts):
        # send all (num_occurrences, word) pairs to the same reducer.
        # num_occurrences is so we can easily use Python's max() function.
        yield (None, (sum(counts), word))
        # discard the key; it is just None
        
    def reducer_find_max_word(self, _, word_count_pairs):
        # each item of word_count_pairs is (count, word),
        # so yielding one results in key=counts, value=word
        h = []
        for (count, word) in word_count_pairs:
            #count = item[0]
            #word = item[1]
            heapq.heappush(h, (-count, word))
        for i in range(0,100):
            yield (heapq.heappop(h))
        
if __name__ == '__main__':
    MRMostUsedWord.run()    