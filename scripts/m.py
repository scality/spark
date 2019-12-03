import struct

bigint = 0xFEDCBA9876543210FEDCBA9876543210L
print bigint,hex(bigint).upper()

cbi = struct.pack("!QQ",bigint&0xFFFFFFFFFFFFFFFF,(bigint>>64)&0xFFFFFFFFFFFFFFFF)

print len(cbi)
