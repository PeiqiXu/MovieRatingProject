from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("assignment1")
sc = SparkContext(conf=conf)

def parseLine(line):
    field=line.split(',')
    customerID = field[0]
    productID = field[1]
    productPrice = float(field[2])
    return (customerID, productID, productPrice)


lines = sc.textFile("/Users/pqxu/SparkCourse/customer-orders.csv")
parseLines = lines.map(parseLine)
temp = parseLines.map(lambda x:(x[0],x[2]))
sumLines = temp.reduceByKey(lambda x,y:y+x)
sumLinestemp = sumLines.map(lambda x: (x[1],x[0])).sortByKey()
sortedLines = sumLinestemp.map(lambda x: (x[1],x[0]))




results = sortedLines.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))
