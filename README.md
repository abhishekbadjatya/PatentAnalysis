# PatentAnalysis
Team no. - 201712-31

## Data:

1. Please download the following files from http://www.patentsview.org/download/:
  * patents.zip - [link](http://www.patentsview.org/data/20170808/patent.zip)
  * assignees.zip - [link](http://www.patentsview.org/data/20170808/assignee.zip)
  * patent\_assignee.zip - [link](http://www.patentsview.org/data/20170808/patent_assignee.zip)

2. Unzip the downloaded files to a sub-folder named 'data'.

3. Ensure that the 'SPARK_HOME' environment variable is set to the Spark installation directory.

4. Ensure that the Graphene docker images are running for the address localhost:8010


## Code Structure

    ├── data                   # I/O data required for the scripts.
    ├── src                    # Scripts that comprise the project.
    
## Order of Executing scripts:

1. src\\patents\_sanitization.py
2. src\\patents\_analysis.py
3. src\\run\_all.sh

