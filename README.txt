Name: Jyothirmayi panda
ID: jpanda@uncc.edu

Files Information
This zipped file consists of four Java files.

1. DocWordCount.java
2. TermFrequency.java
3. TFIDF.java --> The outputfile of TFIDF has to be saved in a .txt file for giving it as input in the next step (Search.java)
4. Search.java --> Takes the file (saved in step3) as input , query words as command line arguments seperated by space (Adam fireplace book) and produces the output with TFIDF scores of the files.
4. Ranker.java --> This program takes the input of search.java file and returns the descending order of TFIDF scores. 

Approach for Ranker.java:
========================

Mapper takes input of Search.java file. The input is in the format <filename	TFIDF-score>
The mapper always generates the output in increasing key order. So I multiple the TFIDF scores by -1 and given them as keys and filename as values.

The reducer receives the output of mapper as <negative value of TFIDF filename>
The reducer produces the output in the order it receives from the mapper.
Here I made the reducer to be only one.
In the reducer step, the absolute value of the TFIDF score is taken, so negative sign is removed. And the output will be in the descending order of keys, as received from mapper.

Example:
Input file:
a	1.0987
b	0.898765
c	1.4234
d	1.065

Output from mapper:
-1.4234		c
-1.0987		a
-1.065		d
-0.898765	b

Output from Reducer
1.4234  c
1.0987  a
1.065   d
0.898765        b




