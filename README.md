# Textual Analysis for Automated Phrase Correction (TAAPC)

Implemented a collection of Map Reduce jobs to parse text from a collection of books and compute the probability of each word’s occurring antecedent to each of the other words in the sentence. The application uses the extracted knowledge to correct a given sentence by removing, adding, or changing the order of words.

### cs455.BigramCreator 
 It is a mapReduce job that create bigrams  from text read from a collection of books.
 
### cs455.ProbabilityCalculator
It is a mapReduce job that uses the created bigrams to computes the probability of each word’s occurring antecedent to each of the other words.

### cs455.AddMissingWord
It is a mapReduce job that uses the computed probabilities to find the position of the missed word and then finds the word that fits in.

### cs455.AddMissingWord
It is a mapReduce job that uses the computed probabilities to find the position of the missed word and then finds the word that fits in.

### cs455.RemoveRedundantWord
It is a mapReduce job that uses the computed probabilities to find the word with minimum probability  and then remove it from the sentewnce.

### Swapping two words in a sentance
Both (cs455.RemoveRedundantWord, s455.RemoveRedundantWord) can be used to find the words in the wrong positions and then swap them to their right positions.
