import nltk
from nltk.data import find

try:
    find('sentiment/vader_lexicon.zip')
    print("it existss")
except LookupError:
    nltk.download('vader_lexicon')
print("hellow world")
