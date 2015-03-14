import sys
filename = open("big.tsv", "w")
x=0
while x<(sys.maxint/2000):
	filename.write("%i\t%i\n" % (x, x))
	x+=1