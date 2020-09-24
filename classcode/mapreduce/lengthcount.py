from mrjob.job import MRJob

class LengthCount(MRJob):
    """ for every x, we want to know how many appearances of x letter words there are
    e.g., number of appearnces of 2 letter words, number of appearances of 3 letter words
    """

    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (len(w), 1)

    def reducer(self, key, values):
        yield (key, sum(values))

if __name__ == '__main__':
    LengthCount.run()
