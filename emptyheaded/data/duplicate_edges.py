import sys

read_file = sys.argv[1]
out_file = sys.argv[2]

sep = None
if len(sys.argv) > 3:
    sep = sys.argv[3]

f = open(read_file, 'r')
w2 = open(out_file, 'w')
s = set()

for line in f:
    line = line.strip()
    nodes = line.split() if not sep else line.split(sep)
    source = int(nodes[0])
    dest = int(nodes[1])
    tup = (source,dest)
    if tup not in s:
		s.add(tup)
		s.add((dest,source))
		w2.write(str(source) + ' ' + str(dest) + '\n')
		w2.write(str(dest) + ' ' + str(source) + '\n')
w2.close()
f.close()
