#!/usr/bin/env python
import sys


def verify(counter, words):
    for k, v in words.items():
        print "Found:\t", k, "\t", v
        counter -= v
    if counter != 0:
        raise Exception("Missing %s words", counter)

def count_words():
    words = {}
    try:
        counter = 0
        with open('words.data', 'r') as f:
            words_read = f.read().split(',')
            counter = len(words_read)
            print "Read: ", counter
            for w in words_read:
                if words.has_key(w):
                    words[w] += 1
                else:
                    words[w] = 1
        verify(counter,words)
    except Exception as e:
        print "Error reading words: ", e
        sys.exit(1)


def main():
    print "Reading words.data"
    count_words()

if __name__ == "__main__":
    main()
