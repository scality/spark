import re

def sparse(f):
	lst  = []
	m = re.findall(r'(200000000000014|20100000014)([0-9-a-f]{40})',f)
	n = re.findall(r'(200000000000013|20100000013)([0-9-a-f]{38})',f)
	for mm in m:
		key = mm[1]
		lst.append(key.upper())
	for nn in n:
		key = nn[1].zfill(38)
		lst.append(key.upper())
	return lst

f = open("/tmp/get")
data = f.read()

chunkshex =  data.encode('hex')
print chunkshex.upper()
print sparse(chunkshex)

