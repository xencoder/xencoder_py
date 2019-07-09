#Progress bar in Python
import sys
#Alternative is to use:
#pip install progressbar

def makeArrow(nbr):
    ind_prog = "="*(nbr-1)
    ind_prog = ind_prog+"=>"
    return ind_prog

results = ListOfObjects()

for row in results:
    sys.stdout.write('\r')
    sys.stdout.write("[%-1s] %d%%" % (makeArrow(int(percentage)), percentage))
    sys.stdout.flush()
