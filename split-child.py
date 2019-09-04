import re

def sparse(f):
	lst  = []
	m = re.findall(r'(425a2d44420100000014|80000000002000000000000..)([0-9-a-f]{40})',f)
	for i in m:
		lst.append(i[1].upper())
	return lst

f = open("/tmp/get")
data = f.read()

chunkshex =  data.encode('hex')
print chunkshex.upper()
print set(sparse(chunkshex))
print len(sparse(chunkshex))
print len(set(sparse(chunkshex)))

