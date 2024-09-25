<h1>City List Assignment - Answers</h1>

<h2>Question 1</h2>
What is the count of all rows?   
2583   
I used the len function (length of the dataframe) to check how many rows are in the dataframe with all the files after loading with no manipulation.

<h2>Question 2</h2>
What is the city with the largest population?  
Mumbai (Bombay)  
I used the idxmax function on the Population column to find the index of the row with the max number in the column. Then I printed the Name of the city in that index row.

<h2>Question 3</h2>
What is the total population of all cities in Brazil (CountryCode == BRA)?  
55955012.0  
I used the groupby function with sum and filter of CountryCode == BRA

<h2>Question 4</h2>
What changes could be made to improve your program's performance.  
If the script needs to handle many files, running the script will take very long. To run the loading in parallel I can use the multiprocessing python package to load the files in parallel.  
This is the only part of the script that can be done in parallel, as the manipulation on the dataframe are done once.  
Additionally, I can explore other packages to load the data faster.

<h2>Question 5</h2>
How would you scale your solution to a much larger dataset (too large for a single machine to store)?  
There are many options to handle this case, all solutiosn will require a system that can support distributed computing, as one machine will not be able to handle the data.  
Since my script uses python packages and especially pandas, it would be the easiest to convert the code to use pyspark and its built-in panda's data frames.
