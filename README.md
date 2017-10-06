# MapReduce-and-Spark
Hadoop MapReduce program, TwoPhase.java, that computes the multiplication of two given matrices using the twophase approach

## Hadoop Two Phase.java
## Input:
Two matrices as mat-A and mat-B, refer example folder, each line of which is a tuple (row-index, column-index,
value) that specifies a non-zero value in the matrix.

## Output Format:
The entries of matrix C (= A * B) in the following format (tab-separated):
1,1 3
1,2 7
2,1 2
2,2 4
3,1 3
3,2 3

## Running Instructions:
<li> Need a spark installation on an AWS local client </li>
<li> Create Jar file as: </li>

1. bin/hadoop com.sun.tools.javac.Main TwoPhase.java

2. jar cf 2p.jar TwoPhase*.class

<li> Generate output from the jar file by running on input:

3. bin/hadoop jar 2p.jar TwoPhase mat-A mat-B output

## TwoPhase.py
Python Spark code for performing two phase multiplication of two matrices.

## Running Instructions:
<li> Same input as before. </li>

bin/spark-submit TwoPhase.py mat-A/values.txt mat-B/values.txt output.txt

<li> output file created as output.txt, under the parent directory inside Spark. </li>







