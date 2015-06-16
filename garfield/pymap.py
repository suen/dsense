import sys

def map():

	stopwords = [ x.strip() for x in open("./concepts/stopword.txt").readlines()]

	lines = [ x.strip() for x in open(sys.argv[1]).readlines() ]
	
	characters = "( ) : ? , .".split(" ")

	firstbreak = False
	for line in lines:
		
		#line = sys.stdin.readline().strip()
		
		'''
		if line == "":
			if firstbreak:
				break
			firstbreak = True
		'''
		
		for c in characters:
			line = line.replace(c, "")

		line = line.lower().strip()
		
		try:
			line = line.encode('ascii', 'ignore')
		except:
			pass
			continue

		words = line.replace(",", "").replace("  "," ").split(" ")

		for w in words:
			if w in stopwords or len(w) <= 4 or w.find("http") >= 0:
				continue
			print "%s,1"%w
		

if __name__ == "__main__":
	map()
