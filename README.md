Libraries required :
- shutil
- sys
- PySpark

Run Instruction :
```sh
python3 pyspark_[problemnumber].py <num_cores> <output file>
```

Each of the problem write to the output file in exactly the pyspark dataframe format.

For Problem 1 :
- get the number of Airports by Country

Output format : 
```sh
Row(COUNTRY='<COUNTRY>', count=<count>)
```

For Problem 2 :
- find the Country having the highest number of airports

Output format :
```sh
Row(COUNTRY='<COUNTRY>')
```
Answer is United States(US).

For Problem 3 :
- find airports whose latitude is between [10, 90] and longitude is between [-10, -90]

Output format :
```sh
Row(NAME='<AIRPORT_NAME>')
```