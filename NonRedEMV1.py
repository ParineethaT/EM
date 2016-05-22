#Baseline implementation 1
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName("Redundant Entity Matching V0.1").setMaster("local")
sc = SparkContext(conf=conf)

##Remove the header before loading file.

file = sc.textFile("/home/pari/Downloads/HelpfulContacts_RevisedFlatFiles/Agency\ List\ By\ Organization.csv")


###FILE HAS THESE FIELDS.
#1. State Abbreviation,
#2. State,
#3. Organization Name,
#4. Agency Name,
#5. Local Phone,
#6. Local Phone Extension,
#7. Local Phone Notes,
#8. Local TDD,
#9. Local TDD Extension,
#10. Tollfree Phone,
#11. Tollfree Extension,
#12. Tollfree Instate?,
#13. Tollfree Phone Notes,
#14. Tollfree TDD,
#15. Tollfree TDD Extension,
#16. Spanish Phone,
#17. Spanish Phone Extension,
#18. Address,
#19. Address2,
#20. Agency Notes,
#21. E-mail Address,
#22. Web Address
###


mappedFile = file.map(lambda x: x.split(",")[3:5]+x.split(",")[9:10]+x.split(",")[-2:])


## ONLY TAKEN THESE FIELDS, BECAUSE THEY DON'T HAVE MANY NULL VALUES AND THEY ARE NOT COMMON LIKE 'STATE'//
#[u'Agency Name',
# u'Local Phone',
# u'Tollfree Phone',
# u'E-mail Address',
# u'Web Address']
#

#TO GIVE UNIQUE ID TO EACH ROW IN FILE
i = 0L
def prefix_id(lines):
	global i
	i += 1
	return str(i), lines



## Giving unique ID to each row. Instead of matching two rows matching two row IDs serves the same purpose
##having to shuffle rows.

indexedFile = mappedFile.map(prefix_id)
#BACK UP INDEXED FILE FOR FUTURE REF..//
indexedFile.saveAsTextFile("/home/pari/Desktop/indexedFile")
# SCHEMA: [ID,[AGENCY, LOCALPHONE, TOLLFREEPHONE, EMAIL, WEB]]


##MapValues function to pickup the fields, filter function to discard nulls in agency name and
#map function to make agency name, key and ID, value. 
agency = indexedFile.mapValues(lambda x: x[0]).filter(lambda (u,v): v!='').map(lambda (x,y): (y, x))

#AFTER: [AGENCY, Row_ID]
#DISCARDED ALL THE NULL VALUES


#Doing the same thing for other attributes.
localPhone = indexedFile.mapValues(lambda x: x[1]).filter(lambda (u,v): v!='').map(lambda (x,y): (y, x))
#AFTER: [LOCALPHONE, Row_ID]

tfPhone = indexedFile.mapValues(lambda x: x[2]).filter(lambda (u,v): v!='').map(lambda (x,y): (y, x))

email = indexedFile.mapValues(lambda x: x[3]).filter(lambda (u,v): v!='').map(lambda (x,y): (y, x))

web = indexedFile.mapValues(lambda x: x[4]).filter(lambda (u,v): v!='').map(lambda (x,y): (y, x))


#GROUPBYKEY GIVE ALL IDs FOR A  GIVE VALUE, RETURN ITERABLE OBJECT WHICH NEEDS TO BE CONVERTED INTO LIST.//


#groupByKey: for a given key (agency name) it returns, agency name and an iterable object of all
# the rows that have the same agency name.

## Further converting iterable object into sorted list to be able generate pairs
#out of it, or just to be able to see the id's in the iterable object.

matched_agencies = agency.groupByKey().mapValues(lambda x: sorted(list(x)))
#groupByKey returns key, iterable<value>
#SCHEMA: [AGENCY, SORTED(LIST<ID>)]

matched_emails = email.groupByKey().mapValues(lambda x: sorted(list(x)))
matched_localPhones = localPhone.groupByKey().mapValues(lambda x: sorted(list(x)))
matched_tfPhones = tfPhone.groupByKey().mapValues(lambda x: sorted(list(x)))
matched_web = web.groupByKey().mapValues(lambda x: sorted(list(x)))





#TO GO THROUGH THE LIST AND GENERATE PAIRS
#Take first element and pair that with rest of the elements until there's no element left in list.

##Defining function that generates the pair of ID's out of list of ID's.
def pair(list):
	out = []
	while (len(list) > 0):
		popped = list.pop()
		for x in list:
			out.append(str(popped)+"-"+str(x))
	return out


## Using above defined pair function to work on each sorted list of IDs

##flatMapValues generates the pairs of IDs out of list of IDs, and rather than returning list of pairs,
#puts each pair in new line.
flat_matched_agencies = matched_agencies.flatMapValues(lambda x: pair(x))
## SCHEMA: {AGENCY, ID_PAIR}
#
#//e.g. [(u'California Department of Public Health', '302-290'),
# (u'California Department of Public Health', '302-293'),
# (u'California Department of Public Health', '293-290')]
##


flat_matched_emails = matched_emails.flatMapValues(lambda x: pair(x))
flat_matched_localPhones = matched_localPhones.flatMapValues(lambda x: pair(x))
flat_matched_tfPhones = matched_tfPhones.flatMapValues(lambda x: pair(x))
flat_matched_web = matched_web.flatMapValues(lambda x: pair(x))


#OPTIONAL STEP: UNION OF ALL RDDS // Merging of all outputs into one single file.
union_flat_matched = flat_matched_agencies.union(flat_matched_emails).union(flat_matched_localPhones).union(flat_matched_tfPhones).union(web)
union_flat_matched.saveAsTextFile("/home/pari/Desktop/sparkCodePart1_out")

