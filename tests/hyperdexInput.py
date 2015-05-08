import hyperdex.admin
import hyperdex.client

import sys

def main(argv):
        if len(sys.argv) != 3:
                print 'usage: python hyperdexInput.py input.csv numOfAttributes'
                sys.exit()
        attribs = ""
        subspace = ""
        for i in range(int(sys.argv[2])):
                if( i != (int(sys.argv[2]) -1) ):
                        attribs += "float loc" + str(i) + ", "
                        subspace += "loc" + str(i) + ", "
                else:
                        attribs += "float loc" + str(i) + " "
                        subspace += "loc" + str(i) + " "
                
        
        a = hyperdex.admin.Admin("128.59.146.138", 1982)
        name = sys.argv[1].split(".")
        space = " space %s key name attributes %s subspace %s " % (name[0], attribs, subspace)
        #print space
        a.add_space(space)
        c = hyperdex.client.Client("128.59.146.138", 1982)
        count = 0;        
        for line in open(sys.argv[1],'r'):
                entries = line.split(",")
                key = str(entries[0])
                key = key + str(count);
                print key
                data = {}
                for i in range(1,len(entries)):
                        data["loc"+str(i-1)] = float(entries[i])
                print "c.put(%s, %s, %s)" % (name[0], entries[0], str(data))
                c.put(name[0], key, data)
                count = count + 1

if __name__ == "__main__":
        main(sys.argv)
