# Outlinks-Adjacency-Mapper

Its an open source implementation of generating outlink adjacency list of a given data set of articles using simple map reduce job. The input data should 
be of xml format. 

This implementation take scare of red links in the data set as well i.e It identifies those outlinks which doesn't have any article
in the given data set and marks them as red links. These redlinks doesn't make a part of the adjacency list of articles generated at the end.

We have used 2 Map Reduce job to generate a adjacency list on a given wikipedia data set of articles in xml format.

#Usage

Run the driver WikiPageRanking.java with run configuration parameters i.e.
set the arguments of run configuration to correct input and output path 
