import sys


def reduce():

	lastword = ""
	lastcount = 0 
	
	while True:
		
		line = sys.stdin.readline().strip()
		
		if line == "":
			break

		wf = line.split(",")
		word = wf[0]
		count = int(wf[1])

		if lastword == "":
			lastword = word
			lastcount = 1
			continue

		if lastword != word:
			print "%d %s"%(lastcount,lastword)
			lastword = word
			lastcount = 1
			continue

		lastcount += count 

	print "%d %s"%(lastcount,lastword)

if __name__ == "__main__":
	reduce()
