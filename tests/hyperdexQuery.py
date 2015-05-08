import hyperdex.admin
import hyperdex.client
from time import time
import sys

def main(argv):
        if len(sys.argv) != 4:
                print 'usage: python HyperDexTest.py input.csv numOfAttributes subspacename'
                sys.exit()
                
        
        name = sys.argv[1].split(".")
        c = hyperdex.client.Client("128.59.146.138", 1982)
        
        for line in open(sys.argv[1],'r'):
                count = 0
                entries = line.split(",")
                data = {}
                for i in range(0, len(entries), 2):
                        data['loc%d' % (i/2)] = ((float(entries[i]), float(entries[i+1])))
                #print "c.search(%s, %s)" % (sys.argv[3], str(data))
                start = time()
                results = c.search(sys.argv[3], data)
                c.loop()
                if not results:
                        continue
                try:
                        for x in results:
                                count += 1
                                #print x
                        end = time()
                        print "%d, %f" % (count, (end - start)* 1e3) 
                except AssertionError:
                        print "failure"
        
if __name__ == "__main__":
        main(sys.argv)
