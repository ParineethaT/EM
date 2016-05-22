#!/usr/bin/env python

import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import HiveContext, Row, Window
from pyspark.sql.functions import *
from pyspark.sql.types import *

if len(sys.argv) != 3:                                                                          #O(1)
	
	print(""" 
		Error: This program takes 2 arguments
		Usage: bin/spark-submit --master <spark-master> nrem.py <input dir> <output dir>
		bin/spark-submit --master 192.168.159.129 /home/pari/nrem.py /home/pari/Downloads/PLAID192.csv /home/pari/output
	""")
	sys.exit(1)

conf = SparkConf().setAppName("Non Redundant Entity Matching")
sc = SparkContext(conf=conf)
sqlCtx = HiveContext(sc)

def attr_key(l):
	"""
		[obj, attr1, attr2, attr3 ...] -> [(attr1, obj), (attr2, obj), (attr3, obj) ...]
	"""
	a = []
	for attr in l[1:]:                                                                               O(n* num of attributes)
		a.append(Row(attr=attr, obj=l[0]))
	return a

"""
	Assuming input file(s) to be tsv, and first field to be object and rest of the fields as attributes 
"""
#Read input
inRDD = sc.textFile(sys.argv[1])

##Generating attribute-object pair from each line
aoPair = inRDD.flatMap(lambda line: attr_key(line.split("\t")))                                      # O(N* num of attributes)


##Converting to Dataframe

"""
	Sample Table
	+----+---+
	|attr|obj|
	+----+---+
	|   1|  a|
	|   2|  a|
	|   3|  a|
	|   1|  b|
	|   2|  b|
	|   3|  b|
	|   1|  c|
	|   3|  c|
	+----+---+
"""

schema = StructType([StructField("attr", StringType(), True), StructField("obj", StringType(), True)])      #O(1)

aoDF = sqlCtx.createDataFrame(aoPair, schema)

#Window that moves over rows of same obj and sorted by attr

window = Window.orderBy("attr").partitionBy("obj")

## Prev column contains previous attr of the same object
"""
	Transformed Table	
	+----+---+----+
	|attr|obj|prev|
	+----+---+----+
	|   1|  a|null|
	|   2|  a|   1|
	|   3|  a|   2|
	|   1|  b|null|
	|   2|  b|   1|
	|   3|  b|   2|
	|   1|  c|null|
	|   3|  c|   1|
	+----+---+----+
"""
memorize = aoDF.select("attr", "obj", lag("attr",1, None).over(window).alias("prev"))


##Back to RDD
#
"""
	*
	[(1, (u'a', None)),
	 (2, (u'a', 1)),
	 (3, (u'a', 2)),
	 (1, (u'b', None)),
	 (2, (u'b', 1)),
	 (3, (u'b', 2)),
	 (1, (u'c', None)),
	 (3, (u'c', 1))]
"""
mappedRDD = memorize.map(lambda row: (row.attr, (row.obj, row.prev)))


#Group by 'attr' and collect tuple(obj, prev) into lists
"""
	[(1, [(u'a', None), (u'b', None), (u'c', None)]),
	 (2, [(u'a', 1), (u'b', 1)]),
	 (3, [(u'a', 2), (u'b', 2), (u'c', 1)])]
"""
groupedByAttr = mappedRDD.groupByKey().mapValues(list).cache()

##Calling an action to materialize cache.

groupedByAttr.take(5)

##Function to collect matched and eliminated pairs

def pairFilter(elim=False):
	##Return correct function based on 'elim'

	def matchedPair(l):
		s = set()
		#Two loops to iterate through list of obj, prev
		#Outer loop
		for obj_o, prev_o in l:
			#Inner loop
			for obj_i, prev_i in l:
				#Case for matching
				if obj_o != obj_i and ((prev_o != prev_i) or (not prev_i and not prev_o)):
					#No duplicates
					s.add("-".join(sorted([obj_o, obj_i])))
		return s

	def eliminatedPair(l):
		s = set()
		for obj_o, prev_o in l:
			for obj_i, prev_i in l:
				if obj_o != obj_i and ((prev_o == prev_i) and (prev_o and prev_i)):
					s.add("-".join(sorted([obj_o, obj_i])))
		return s

	"""
		>>> matched = pairFilter(elim=False)
		>>> eliminated = pairFilter(elim=True)
	"""	
	if elim:
		return eliminatedPair
	else:
		return matchedPair
## matched is an instance of matchedPair()
matched = pairFilter(elim=False)
##eliminated is an instance of eliminatedPair()
eliminated = pairFilter(elim=True)
##RDD with all the matched pairs
"""
	***** Problem in the algorithm : if objects skip attributes then you get duplicate pairs across keys/attributes ****
		*** Since 'a' and 'c', and 'b' an 'c' did not have common previous attributes. ***
	[(1, {u'a-b', u'a-c', u'b-c'}), (3, {u'a-c', u'b-c'})]
"""
##Filter to eliminate empty sets
matchedPairs = groupedByAttr.mapValues(lambda x: matched(x)).filter(lambda (x, y): True if len(y) else False)
#RDD with all the eliminated pairs
"""
	****  'a' and 'b' both had common previous attributes ****
	[(2, {u'a-b'}), (3, {u'a-b'})]
"""
##Filter to eliminate empty sets
eliminatedPairs = groupedByAttr.mapValues(lambda x: elimated(x)).filter(lambda (x, y): True if len(y) else False)

##Saving outputs

matchedPairs.saveAsTextFile(sys.argv[2] + "/matched")

eliminatedPairs.saveAsTextFile(sys.argv[1] + "/eliminated")

sc.stop()

## End of Program
