FROM centos:7
MAINTAINER PDS <patrickdos94500@gmail.com>

COPY ./dep /tmp/dep/

RUN yum -y install epel-release  
RUN yum -y install /tmp/dep/scality-*
RUN yum -y install python fuse-libs libicu dlm-lib snappy
RUN curl "https://bootstrap.pypa.io/get-pip.py"  | python 
RUN pip install Flask 
RUN pip install twisted

RUN rm -rf /tmp/deps/


WORKDIR /home/website
RUN mkdir /home/website/webapp/
RUN mkdir /home/website/bin/
RUN mkdir /home/website/webapp/static/
RUN mkdir /home/website/webapp/templates/
COPY ./sparsectl /home/website/bin/
COPY ./sfused.conf /home/website/bin/
COPY ./sparsecmd.py /home/website/bin/
RUN chmod +x /home/website/bin/sparsectl
RUN chmod +x /home/website/bin/sparsecmd.py
COPY ./webapp/static /home/website/webapp/static/
COPY ./webapp/__init__.py /home/website/webapp/
COPY ./run.py /home/website/
COPY ./webapp/templates /home/website/webapp/templates/

ENV PYTHONPATH "${PYTHONPATH}:/home/website"
RUN chmod +x /home/website/run.py
CMD /usr/bin/twistd -n web --port tcp:9999 --wsgi run.app

