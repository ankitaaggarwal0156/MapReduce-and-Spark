import sys
from pyspark import SparkContext
from operator import add

sc = SparkContext(appName="inf551")

def reducefunc(x):
	a_index=list()
	a_val = list()
	b_index= list()
	b_val = list()
	for i in range(len(x[1])):
		if(x[1][i][0]=='A'):
			a_index.append(x[1][i][1])
			a_val.append(x[1][i][2])
		if(x[1][i][0]=='B'):
			b_index.append(x[1][i][1])
			b_val.append(x[1][i][2])
	
	result=[]
	t1 =[]
	for i in range(len(a_index)):
		for j in range(len(b_index)):
			k = a_val[i]*b_val[j]
			t1 = (a_index[i],b_index[j])
			result.append((t1,k))
	return result

lines = sc.textFile(sys.argv[1])
matA = lines.map(lambda x: x.split(',')).map(lambda x: (int(x[1]),('A',int(x[0]), int(x[2]))))

lines1 = sc.textFile(sys.argv[2])
matB = lines1.map(lambda x: x.split(',')).map(lambda x: (int(x[0]),('B',int(x[1]), int(x[2]))))

matC = matA.union(matB).groupByKey().map(lambda x: (x[0], list(x[1])))


filename = sys.argv[3]
f = open(filename,'w')		
matC = matC.map(reducefunc).reduce(lambda x, y: list(x)+list(y))
matC1 = sc.parallelize(matC)
output = matC1.reduceByKey(add)


out = output.map(lambda x: str(x[0][0])+","+ str(x[0][1])+"     "+str(x[1])).collect()

for v in out:
	f.write('%s\n' %v)
	
f.close()










