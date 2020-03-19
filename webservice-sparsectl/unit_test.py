import re
import json
from subprocess import Popen, PIPE
def sparse(key):
        search = re.compile('[A-F0-9].20$')
        search_err = re.compile('(err=.*|SCAL_SPARSE.*)')
        listkey = []
        cmd = "python /home/website/bin/sparsecmd.py --conf /home/website/bin/sfused.conf dump %s" % key
        p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
        stdout, stderr = p.communicate()
        retkey  = stdout.split()
        for i in retkey:
                if search.search(i):
                        listkey.append(i)
        if stderr:
                print stderr
                err = search_err.search(stderr).group(0)
                listkey=[err]
        if len(listkey) == 0:
                listkey=["empty"]
        return json.dumps(listkey)


#empty="DE01B949067B975A"
print(sparse("DE01B949067B975A"))
#eno="8EBAA05D49BBC7DB"
print(sparse("8EBAA05D49BBC7DB"))
#failure="39DF100141297EF2"
print(sparse("39DF100141297EF2"))
#SCAL_DB_EIO
print(sparse("8724F322AF9D9A0A"))
