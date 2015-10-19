#!/usr/bin/env python
import random

def pick_five_random_words():
    retval = []
    try:
        with open('/usr/share/dict/words', 'r') as f:
            lines = f.read().splitlines()
            random.shuffle(lines)
            retval = lines[:5]
    except Exception as e:
        print "Error reading rand words. Defaulting."
        retval = ['foo', 'bar', 'buzz', 'concord', 'mary_joliza']
    return retval

def write_tmp_words_file(words):
    if words is None:
        raise Exception("Invalid words to write to the file")
    with open('words.data', 'w') as f:
        i = 0
        max_index = len(words) - 1
        max_count = 1000000
        while i < max_count:
            i += 1
            f.write(words[random.randint(0, max_index)])
            if i < max_count:
                f.write(',')

        print "Generated: ", max_count, "words"

def main():
    print "Picking random words"
    words = pick_five_random_words()
    print "Words: ", words
    write_tmp_words_file(words)

if __name__ == "__main__":
    main()
