import sys


def clean():
	
	for x in range(int(sys.argv[1])):
		
		line = sys.stdin.readline().strip()
		
		if line == "":
			break

		wf = line.split(" ")
		print wf[1]

if __name__ == "__main__":
	clean()
