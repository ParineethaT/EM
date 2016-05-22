#Baseline implementation 1
import time, socket, threading, hashlib, random
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Non Redundant Entity Matching V2.0")
sc = SparkContext(conf=conf)

raw_file = sc.textFile("/home/pari/Desktop/Agency\ List\ By\ Organization.csv")

mappedFile = raw_file.map(lambda x: x.split(",")[3:5]+x.split(",")[9:10]+x.split(",")[-2:])

def prefix(lines):
  unique = str(random.randint(0, sys.maxint))+str(time.time())+threading.current_thread().name+socket.gethostbyname(socket.gethostname())
  h = hashlib.md5(unique).hexdigest()
  return h, lines

indexedFile = mappedFile.map(prefix).repartition(numPartitions)
##Possible partioning here..

agency = indexedFile.mapValues(lambda x: x[0]).filter(lambda (u,v): v!='')


localPhone = indexedFile.mapValues(lambda x: x[1]).filter(lambda (u,v): v!='')


tfPhone = indexedFile.mapValues(lambda x: x[2]).filter(lambda (u,v): v!='')

email = indexedFile.mapValues(lambda x: x[3]).filter(lambda (u,v): v!='')

web = indexedFile.mapValues(lambda x: x[4]).filter(lambda (u,v): v!='')



union_all = agency.union(localPhone).union(tfPhone).union(email).union(web)

sorted_union = union_all.sortByKey(numPartitions=None)#Partitioned at Line 15
 
prev = None
prev_attr = None
def memorize(row_id, attr):
  global prev
  global prev_attr
  if row_id == prev:
    prev = row_id
    temp = prev_attr
    prev_attr = attr
    return attr, row_id+"|"+temp.encode("utf-8")
  else:
    prev = row_id
    prev_attr = attr
    return attr, row_id+"|"+"None"
  
mapped_sorted = sorted_union.map(lambda (x,y): memorize(x,y)).repartition(numPartitions)##will destroy partitioning information

resorted_mapped = mapped_sorted.sortBy(lambda x: x[1].split("|")[1], numPartitions=None)
##Possible Partitioning here... 

def combAdd(a,b):
  return a+b

prev = None




def createComb(v):
  global prev
  row_id, current = v.split("|")
  if current != prev or current == None:
    return [row_id]
    
      
def mergeValue(v, u):
  global prev
  row_id, current = u.split("|")
  if current != prev or current == None:
    return v + [row_id]

prev = None
def createCombElim(v):
  global prev
  row_id, current = v.split("|")
  if current == prev and current != None:
    prev = current
    return [row_id]
  else:
    prev = current
    return []
      
def mergeValueElim(v, u):
  global prev
  row_id, current = u.split("|")
  if current == prev or current != None:
    prev = current
    return v + [row_id]
  else:
    prev = current
    pass
    

    
matched = resorted_mapped.combineByKey(createComb, mergeValue, combAdd)
eliminated = resorted_mapped.combineByKey(createCombElim, mergeValueElim, combAdd)
matched_rows = matched.filter(lambda (x, y): len(y) > 2)
eliminated_rows = eliminated.filter(lambda (x, y): len(y) > 2)


def pair(list):
    out = []
	while (len(list) > 0):
		popped = list.pop()
		for x in list:
			out.append(str(popped)+"-"+str(x))
	return out


matched_pairs = matched_rows.flatMapValues(lambda x: pair(x))
eliminated_pairs = eliminated_rows.flatMapValues(lambda x: pair(x))
matched_pairs.saveAsTextFile("/home/pari/Desktop/matched")
eliminated_pairs.saveAsTextFile("/home/pari/Desktop/eliminated")
