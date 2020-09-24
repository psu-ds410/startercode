from mrjob.job import MRJob

class LowerCount(MRJob):
    """  get the counts of words that start with lowercase i """

    def mapper(self, key, line):
        words = line.split()
        for w in words:
            if w.startswith('i'):
                yield (w, 1)

    def reducer(self, key, values):
         yield (key, sum(values))

if __name__ == '__main__':
    LowerCount.run()
