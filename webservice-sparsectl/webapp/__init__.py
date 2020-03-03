from flask import Flask, render_template, redirect, url_for, request, session, Response
import datetime
import time
import re
import json
import os
from subprocess import Popen, PIPE, STDOUT

app = Flask(__name__)
app.secret_key = os.urandom(12)

@app.route('/sparse/<key>')
def home(key):
	search = re.compile('[A-F0-9].20$')
	search_err = re.compile('err=.*')
	listkey = []
	cmd = "python /home/website/bin/sparsecmd.py --conf /home/website/bin/sfused.conf dump %s" % key
	p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = p.communicate()
	retkey  = stdout.split()
	for i in retkey:
		if search.search(i):
			listkey.append(i)
	if stderr:
		err = search_err.search(stderr).group(0)
		listkey=[err]
	if len(listkey) == 0:
		listkey=["empty"]
	return json.dumps(listkey)

#empty="DE01B949067B975A"
#eno="8EBAA05D49BBC7DB"
#failure="39DF100141297EF2"
