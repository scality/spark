import hashlib
import binascii
import struct

def pad2(n):
  x = '%s' % (n,)
  return ('0' * (len(x) % 2)) + x

def to_bytes(h):
        return binascii.unhexlify(h) 

def get_digest(name):
      m = hashlib.md5()
      m.update(name)
      digest = bytearray(m.digest())
      return digest

def get_dig_key(name):
      digest = get_digest(name)
      hash_str =  digest[0] << 16 |  digest[1] << 8  | digest[2] 
      oid = digest[3] << 56 |  digest[4] << 48 |  \
            digest[5] << 40 | digest[6] << 32 |   \
            digest[7] << 24 |  digest[8] << 16  | digest[9] << 8 | digest[10] 
      hash_str = "{0:x}".format(hash_str)
      oid = "{0:x}".format(oid)
      oid = oid.zfill(16)
      volid = "00000000"
      svcid = "51"
      specific = "102060"
      cls = "70"
      key = hash_str.upper() + oid.upper() + volid + svcid + specific + cls
      return key.zfill(40)

def gen_md5_from_id(key):
	key = key.lstrip('0')
	key = pad2(key)
	int_b = to_bytes(key)
	return get_dig_key(int_b).upper()

lst = {}
lst["DFB745B781FD8F5F5E832D11B257AB4D18E17720"] = "4A121AC61828A937E32EAF000000005110206070"
lst["1F6C11FDDC2F44218C02FA044147394DE4FF8900"] = "046167949F32A09C353918000000005110206070"
lst["C13DBBC8D95B4E83DDFEB24AE4F9944DFF961400"] = "A60BC9D1037CAEB2D3A317000000005110206070"
lst["DC22D1687DF093718EF68832D38FE94D38B57100"] = "C0974FF721E5B479A4B894000000005110206070"
lst["A05AC5A8898431110B10436178757E4D4AEF9C20"] = "4E4982A2413CE2B1AC585B000000005110206070"
lst["C5FEF5E77AAA4880A91D8CF5050EB84DB4B47300"] = "DF21350624968C7B6AC4A5000000005110206070"
lst["3632122526CF764735B024EB3ED54DCC2E5900"] = "6B6AEDCC8B0E9E59CCCA14000000005110206070"
lst["7076C0E2B0F13D7FE8A566FBAB714DB9B1CF00"] = "6E33D103B9CA3319F785D4000000005110206070"
lst["9793B06BA9006D0E24CE26B8A02DC4D68A42900"] = "F8A85FB4A2F5B08B1F8494000000005110206070"
lst["E5A6F86148A0A9AB055966822D454DD018BF00"] = "EE71CCFB675BEB92A3687E000000005110206070"
lst["35EC2ECEE4AD0EE6925A81CE25250801B28620"] = "DB8AC8E4E99B10324AA389000000005110206070"
lst["122526CF764735B024EB3ED54DCC2E5900"] = "55A0249F5F59A3AF58AB79000000005110206070"
lst["0035EC2ECEE4AD0EE6925A81CE25250801B28620"] = "DB8AC8E4E99B10324AA389000000005110206070"
lst["000000122526CF764735B024EB3ED54DCC2E5900"] = "55A0249F5F59A3AF58AB79000000005110206070"

for i in lst:
	print i
	print gen_md5_from_id(i) , lst[i]
	assert gen_md5_from_id(i) == lst[i]
gen_md5_from_id("35EC2ECEE4AD0EE6925A81CE25250801B28620")
gen_md5_from_id("0035EC2ECEE4AD0EE6925A81CE25250801B28620")
