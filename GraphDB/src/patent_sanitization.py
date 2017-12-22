import os
import findspark
import pyspark
from pyspark import SparkContext, SQLContext
import pyspark.sql
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, StopWordsRemover, RegexTokenizer, Word2Vec


# Explicitly add SPARK_HOME environment variable so that findspark works properly
#os.environ['SPARK_HOME'] = '/home/pratyus/spark/spark-2.2.0-bin-hadoop2.7'

# findspark package is initialized to find the default installation location of pyspark and link with it
findspark.init()

# Create SparkContent and SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)

##################
#HELPER FUNCTIONS#
##################

# Reads a .tsv file into a pySpark dataframe.
def readtsv(filename, sep='\t', header=True):
    data = sqlContext.read.csv('../data/' + str(filename) + '.tsv', sep, header)
    return data


# Selects only specific column from the Dataframe. columns should be a list of column name strings. 
def selectColumns(table, columns):
    filt = table.select(*columns)
    return filt

# Writes the dataframe to csv.
def writecsv(table, filename, header=False, mode='overwrite'):
    table.write.csv('../data/' + str(filename) + '.csv', header=header, mode=mode)

# Creates a new string column from by concatenating elements from an array column
def withArrToStrColumn(table, array_col, str_col):
    from pyspark.sql.functions import udf
    from pyspark.sql.types import StringType

    def array_to_string(my_list):
        return '[' + ' '.join([str(elem) for elem in my_list]) + ']'

    array_to_string_udf = udf(array_to_string,StringType())
    return table.withColumn(str_col, array_to_string_udf(table[array_col]))

#############
#DATA IMPORT#
#############

# Read the patents, assignees and patent_assignees tables to form respective dataframes.
patents = readtsv('patent')
assignees = readtsv('assignee')
patent_assignee = readtsv('patent_assignee')

#####################
#PATENT SANITIZATION#
#####################

# Select columns which are needed for further analysis
patents_rel = selectColumns(patents, ['id', 'type', 'date', 'abstract', 'title', 'num_claims'])

# Select only utility patents from the dataset.
patents_utility = patents_rel.filter("type=='utility'").filter(patents_rel['abstract'].isNotNull())

# Create a column full_text with concatenated title and abstract.
patents_utility = patents_utility.withColumn('full_text', pyspark.sql.functions.concat(pyspark.sql.functions.col('title'), pyspark.sql.functions.lit(' '), pyspark.sql.functions.col('abstract')))

# Create regexTokenizer and stopWordsRemover stages
regexTokenizer = RegexTokenizer(inputCol="full_text", outputCol="words", pattern="[^A-Za-z]+", toLowercase=True)
stopWordsRemover = StopWordsRemover(inputCol="words", outputCol="filtered_words")

# Transform the data using the pipeline.
patents_utility = regexTokenizer.transform(patents_utility)
patents_utility = stopWordsRemover.transform(patents_utility)

# Split the date date column into separate columns for year, month and day.
patents_utility = patents_utility.withColumn('date_split', pyspark.sql.functions.split(patents_utility['date'], '-'))

patents_utility = patents_utility.withColumn('year', pyspark.sql.functions.col('date_split')[0])
patents_utility = patents_utility.withColumn('month', pyspark.sql.functions.col('date_split')[1])
patents_utility = patents_utility.withColumn('day', pyspark.sql.functions.col('date_split')[2])

# Get only the patents from the last 10 years.
patents_10 = patents_utility.filter(patents_utility['year']>'2007')

# Select only useful columns
patents_10 = selectColumns(patents_10, ['id', 'year', 'month', 'day', 'filtered_words'])

# Drop N/A values. This is performed in the end as we don't want to drop more rows than required.
patents_10 = patents_10.na.drop()

#######################
#ASSIGNEE SANITIZATION#
#######################

# Concatenate columns - first name and last name to form the full name
assignees.registerTempTable('assignees')
assignees_1 = assignees.withColumn('full_name', pyspark.sql.functions.concat(pyspark.sql.functions.col('name_first'), pyspark.sql.functions.lit(' '), pyspark.sql.functions.col('name_last')))

# Select columns which are needed for further analysis
assignees_2 = assignees_1.select('id', 'organization', 'full_name')

# Create a new column Assignee which by coalescing the organization and full name columns - this basically fills missing values in the organization column with corresponding value from full name
assignee_3 = assignees_2.withColumn('Assignee', pyspark.sql.functions.coalesce(assignees_2.organization, assignees_2.full_name))

# Keep only assignee id - Assignee name mapping.
assignee_4 = assignee_3.select('id', 'Assignee')

##############################
#PATENT-ASSIGNEE SANITIZATION#
##############################

# Filter patent-assignee relationships to include only those of interest, i.e of type utility from the last 10 years.
patents_assignee_10 = patent_assignee.join(patents_10, patents_10.id == patent_assignee.patent_id, 'inner')

#############
#DATA EXPORT#
#############

patents_assignee_10 = patents_assignee_10.select('patent_id', 'assignee_id')
patents_10 = patents_10.select('id', 'year', 'month', 'day')
assignee_4 = assignee_4.select('id', 'Assignee')

writecsv(patents_assignee_10, 'cleaned_patent_vertices_edges')
writecsv(patents_10, 'cleaned_patents_vertices')
writecsv(assignee_4, 'cleaned_assignee_vertices')







