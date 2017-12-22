import os
import findspark
import pyspark
from pyspark import SparkContext, SQLContext
import pyspark.sql

import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# findspark package is initialized to find the default installation location of pyspark and link with it
findspark.init()

# Create SparkContent and SQLContext
sc = SparkContext()
sqlContext = SQLContext(sc)

##################
#HELPER FUNCTIONS#
##################

# Reads a .tsv file into a pySpark dataframe.
def readcsv(filename, sep=',', header=True):
    data = sqlContext.read.csv('../data/' + str(filename) + '.tsv', sep=sep, header=header)
    return data


# Selects only specific column from the Dataframe. columns should be a list of column name strings. 
def selectColumns(table, columns):
    filt = table.select(*columns)
    return filt

# Writes the dataframe to csv.
def writecsv(table, filename, header=False, mode='overwrite'):
    table.write.csv('./data/' + str(filename) + '.csv', header=header, mode=mode)


#############
#DATA IMPORT#
#############

# Read the patents, assignees and patent_assignees tables to form respective dataframes.
patents = readcsv('cleaned_patent_vertices')
assignees = readcsv('cleaned_assignee_vertices')
patents_assignees = readcsv('cleaned_patent_vertices_edges')

##########################
#TREND ANALYSIS - BY YEAR#
##########################

# Group patents by year.
patents_year = patents.groupby(patents.year).count()

# Change the pySpark Dataframe to Pandas.
patents_year_df = patents_year.toPandas()

# Sort by year in ascending order
patents_year_df = patents_year_df.sort_values(by='year', ascending=True)

# Plot the distribution
sns.set_style("darkgrid")
sns.barplot(x='year', y='count', data=patents_year_df)
plt.xticks(rotation=90, fontsize=7)
plt.savefig('../plots/year_dist.png')

###########################
#TREND ANALYSIS - BY MONTH#
###########################

# Group patents by year.
patents_month = patents.groupby(patents.year, patents.month).count()

# Change the pySpark Dataframe to Pandas.
patents_month_df = patents_month.toPandas()

# Create a new column formed by concatenating the year and the month in YYMM format.
patents_month_df['year_month'] = patents_month_df[['year', 'month']].apply(lambda x: ''.join(x), axis=1)

# Sort by YYMM in ascending order
patents_month_df = patents_month_df.sort_values(by='year_month', ascending=True)

# Plot the distribution
sns.set_style("darkgrid")
plt.plot('year_month','count', data=patents_month_df)
plt.xlabel('Time')
plt.savefig('../plots/month_dist.png')

#######################
#ASSIGNEE DISTRIBUTION#
#######################

# Add a column of assignee name to the patent-assignee relationships table by taking an inner join.
patent_with_assignees = patents_assignees.join(assignees, patents_assignees.assignee_id == assignees.id, 'inner')

# Group patents by Assignees.
patents_assignee = patent_with_assignees.groupby(patent_with_assignees.Assignee).count()

# Change the pySpark Dataframe to Pandas.
patents_assignees_df = patents_assignee.toPandas()

# Sort by count of assignees in descending order
patents_assignees_df = patents_assignees_df.sort_values(by='count', ascending=False)

# Keep only the top 10 assignees
patents_assignees_top10 = patents_assignees_df.head(10)

# Plot the distribution
sns.set_style("darkgrid")
plt.barplot(x='Assignee', y='count', data=patents_assignees_top10)
plt.xlabel('Assignee')
plt.savefig('../plots/assignee_dist.png')






















