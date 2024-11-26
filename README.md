## README  

### Build Instructions  
Run the main.py file to run the consumer program.

### Notes
If no request bucket/queue is specified, then the program will use the default request bucket.  
If no dynamodb table name is specified, then the program will use the default dynamodb table.  
A request bucket and a request queue cannot both be specified.  
The request queue (-rq) argument expects the name of the request queue __NOT__ the url.

#### Arguments
* -rb request bucket
* -rq request queue
* -dwt dynamo widget table 

Example program invocations

python main.py -rb <request bucket> -dwt <dynamodb table name>

python main.py -rq <request queue name> -dwt <dynamodb table name>

