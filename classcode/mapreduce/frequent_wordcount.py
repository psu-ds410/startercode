from mrjob.job import MRJob

class FrequentWordCount(MRJob):
    """ Compute the count of each word
    but only show results for words that occurred at least 3 times
    """
    def mapper(self, key, line):
        words = line.split()
        for w in words:
            yield (w, 1)

    def reducer(self, key, values):
        """ values is an iterator that we can read just once
        if we try to read it a second time, it will be empty
        the second time
        """
        total = sum(values) # after this command, values will become empty
        if total >= 3:
            yield (key, total)

if __name__ == '__main__':
    FrequentWordCount.run()
