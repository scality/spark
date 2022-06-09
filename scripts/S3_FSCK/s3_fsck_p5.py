#!/usr/bin/python2.6
'''
Read keys from stdin and tries to find them by running listKeys on their node.
'''

import sys, os, getopt , re

sys.path.insert(0,'scality')
import subprocess

from scality.supervisor import Supervisor
from scality.daemon import DaemonFactory , ScalFactoryExceptionTypeNotFound
from scality.common import ScalDaemonExceptionCommandError
from scality.key import Key

def usage(output):
    output.write("""Usage: %s [options]
        Options:
        -h|--help                    Show this help message
        -r|--ring ring name
        -s|--supurl Supervisor Url
        -k|--key objID key of the RS2 object to undelete

""" % os.path.basename(sys.argv[0]))

if __name__ == "__main__":
    options="hr:s:k:"
    long_options=["help", "ring=","supurl=","key="]

    try:
        opts, args = getopt.getopt(sys.argv[1:], options, long_options)
    except getopt.GetoptError, err:
        sys.stderr.write("getopt error %s" % err)
        usage(sys.stderr)
        sys.exit(2)

    ring = None
    sup = None
    key = None
    login = "root"
    password = "admin"
    for o, a in opts:
        if o in ("-h", "--help"):
            usage(sys.stdout)
            sys.exit(0)
        elif o in ("-r", "--ring"):
            ring = a
        elif o in ("-s", "--supurl"):
            sup = a
        elif o in ("-k", "--key"):
            key = Key(a)
        else:
            usage(sys.stderr)
            sys.exit(2)

    if not ring:
        usage(sys.stderr)
        sys.exit(2)

    if not sup:
	sup = 'http://127.0.0.1:5580'


    s = Supervisor(url=sup,login=login,passwd=password)
    nodes = {}
    success = True
    node =  None
    arck = None

    for n in s.supervisorConfigDso(dsoname=ring)['nodes']:
	nid = '%s:%s' % (n['ip'], n['chordport'])
	nodes[nid] = DaemonFactory().get_daemon("node", login=login, passwd=password, url='https://{0}:{1}'.format(n['ip'], n['adminport']), chord_addr=n['ip'], chord_port=n['chordport'], dso=ring)
	if not node: node = nodes[nid]

    print "Key to Analyse:", key.getHexPadded()
    v = subprocess.Popen('scalarcdig -b '+nid+' '+key.getHexPadded() , shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in v.stdout.readlines():
	if "objkey"  in line:
		try:
			sarc = Key(line[8:])
		except Exception as e:
			print "RINGFAIURE" , e
			break
		key_list = [ sarc ] + [ x for x in sarc.getReplicas() ]
	        for arck in key_list :
			check = nodes[node.findSuccessor(arck.getHexPadded())["address"]]
			tab  = check.checkLocal(arck.getHexPadded())
			print  "%s;%s;%s" % ( key.getHexPadded() , arck.getHexPadded() , tab )
			if tab["deleted"] == True:
				print "Undelete Key " , arck.getHexPadded()
				version = int(tab["version"]+64)
				try:
				    check.chunkapiStoreOp(op="undelete", key=arck.getHexPadded(), extra_params={"version": version})
				except ScalFactoryExceptionTypeNotFound as e:
				     print "Error %s " , e
			if tab["status"] == "exist":
				key_to_get = arck.getHexPadded()


    try:
	    f = open ("/tmp/"+str(key_to_get),"w+")
    except IOError as e:
		print e
    try:
	    check = nodes[node.findSuccessor(key_to_get)["address"]]
	    check.getLocal(key_to_get, f)
	    f.close()

    except ScalDaemonExceptionCommandError as e:
	    print "GetLocal Error %s " , e
	    sys.exit(0 if success else 1)


    p = subprocess.Popen('scalsplitparseindex -f /tmp/'+str(key_to_get), shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for line in p.stdout.readlines():
		    try:
			    keyline = Key(line)
		    except ValueError as e :
			    if "LSPLIT" in e:
				    print e
				    sys.exit(0 if success else 1)
		    v = subprocess.Popen('scalarcdig -b '+nid+' '+line , shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
		    for line in v.stdout.readlines():
			    if "objkey"  in line:
				try:
					sarcs = Key(line[8:])
				except ValueError as e :
					print "SCALRING FAILURE" , e
					break
				key_list = [ sarcs ] + [ x for x in sarcs.getReplicas() ]
				for arcs in key_list :
					check = nodes[node.findSuccessor(arcs.getHexPadded())["address"]]
					tab = check.checkLocal(arcs.getHexPadded())
					print  "%s;%s;%s" % ( keyline.getHexPadded() , arcs.getHexPadded() , tab )
					if tab["deleted"] == True:
						print "Undelete Key " , arcs.getHexPadded()
						version = int(tab["version"]+64)
						try:
						    check.chunkapiStoreOp(op="undelete", key=arcs.getHexPadded(), extra_params={"version": version})
						except ScalFactoryExceptionTypeNotFound as e:
						     print "Error %s " , e
			    retval2 = v.wait()
    retval = p.wait()
    os.remove("/tmp/"+str(key_to_get))

    sys.exit(0 if success else 1)