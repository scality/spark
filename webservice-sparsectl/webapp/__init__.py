from flask import Flask, render_template, redirect, url_for, request, session, Response, g
import datetime
import time
import re
import json
import os
import logging
from twisted.logger import Logger
from subprocess import Popen, PIPE, STDOUT

app = Flask(__name__)
app.secret_key = os.urandom(12)
logger = Logger()

@app.before_request
def before_request():
    g.request_start_time = time.time()
    g.request_time = lambda: "%dms" % ( int( (time.time() - g.request_start_time)*1000))

@app.route('/sparse/<key>')
def home(key):

	page = request.args.get('option', default = False, type = str)
	full = request.args.get('full', default = False, type = str)
	if page == "key":
		key = key[6:22]
	elif page == "inode":
		key = "%x" % int(key)
	search = re.compile(r'stripe.*key.*([A-F0-9].20$)')
	search_err = re.compile(r'(err=.*|SCAL_SPARSE.*)')
	listkey = []
	cmd = "python /home/website/bin/sparsecmd.py  --conf /home/website/bin/sfused.conf dump %s" % key
	p = Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE)
	stdout, stderr = p.communicate()
	retkey  = stdout.split('\n')
	reterr = stderr.split()

	if full:
		return json.dumps(''.join(retkey))

	for i in retkey:
		if search.search(i):
			key = i.split()[3]
			listkey.append(key)

	ss = search_err.search(str(reterr))
	if ss:
		err = ss.group(0).split()
		listkey=[err[0]]
	if len(listkey) == 0:
		listkey=["empty"]
	return json.dumps(listkey)

@app.after_request
def after_request(response):
    logger.info(
	"{method} {uri} {status} {time}"
	.format(method=request.method, uri=request.path, status=response.status, time=g.request_time())
	)
    return response

