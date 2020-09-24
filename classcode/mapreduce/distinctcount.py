from mrjob.job import MRJob

class DistinctCount(MRJob):
    """ for every x, we want to know how many distinct x letter words there are (do not count duplicates) """

    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (len(w), w) # key = word length, value = word

    def reducer(self, key, values):
        wordset = set(values)
        yield (key, len(wordset))

if __name__ == '__main__':
    DistinctCount.run()
