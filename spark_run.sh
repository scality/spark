#!/bin/bash

[ -z "$1" ] && echo "Usage: $0 <start|stop|status>" && exit 1

# Pleasze change the IPs accordingly to your architecture
master="10.160.187.146"
workers="10.160.169.142 10.160.173.126 10.160.174.196"

local_ips=$(ip ad |grep inet |grep -Ev "127.0.0.1|::" |awk '{print $2}')

local_master=""
if echo "$local_ips" |grep -Fqw $master ; then local_master=$master ; fi

local_worker=""
for worker in $workers ; do
    if echo "$local_ips" |grep -Fqw $worker ; then local_worker=$worker ; fi
done

container_command="$(basename "$(type -p docker || type -p ctr)")"
test -z "$container_command" && echo "docker or CTR not found!" && exit 1

case $1
    in
    start) echo "Starting Spark node"
        case $container_command
            in
            docker) echo "Running $container_command"
                # hosts update
                host_storage=""
                count=01
                for i in $workers ; do host_storage+=" --add-host=storage-$(printf "%02d" ${count}):$i"; ((count++)) ; done
                
                if [ -n "$local_master" ] ; then
                    echo "Running master here"
                    $container_command run --rm -dit --net=host --name spark-master \
                    --hostname=spark-master \
                    --add-host=spark-master:$master \
                    "$host_storage" \
                    registry.scality.com/spark/spark-container:3.5.2 \
                    master
                fi
                
                if [ -n "$local_worker" ] ; then
                    echo "Running worker here"
                    $container_command run --rm -dit --net=host --name spark-worker \
                    --hostname=spark-worker \
                    --add-host=spark-master:$master \
                    --add-host=spark-worker:"$local_worker" \
                    --mount='type=bind,source=/root/spark,target=/root/spark,options=rbind:rw' \
                    "$host_storage" \
                    -v /var/tmp:/tmp \
                    registry.scality.com/spark/spark-container:3.5.2
                    worker
                fi
            ;;
            ctr) echo "Running $container_command"
                # hosts update
                host_storage=""
                count=01
                localname=
                for i in $workers ; do
                    n=storage-$(printf "%02d" ${count})
                    ((count++))
                    if echo "$local_ips" |grep -Fqw "$i" ; then localname=" --host $n" ; fi
                    test "$(grep -qw "$n" /etc/hosts ; echo $?)" == 0 && continue
                    host_storage+="$i $n\n"
                done
                test -n "$host_storage" && echo -e "# Host file updated for Spark\n$host_storage" >> /etc/hosts
                
                test "$(grep -qw spark-master /etc/hosts ; echo $?)" == 0 || \
                echo "$master spark-master" >> /etc/hosts
                
                if [ -n "$local_master" ] ; then
                    echo "Running master here"
                    # remove if exists, throw error if running
                    c=$($container_command c ls |grep -w spark-master)
                    echo "Checking if the container aleady exists"
                    test -n "$c" && \
                    $container_command c rm spark-master
                    
                    # start
                    echo "
                    $container_command run -d --net-host \
                    --env='SPARK_NO_DAEMONIZE=true' \
                    --mount='type=bind,src=/root/spark-apps,dst=/opt/spark/apps,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-data,dst=/opt/spark/data,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-logs,dst=/opt/spark/spark-events,options=rbind:rw' \
                    registry.scality.com/spark/spark-container:3.5.2 spark-master \
                    ./entrypoint.sh master
                    " |tr -s ' '
                    $container_command run -d --net-host \
                    --env='SPARK_NO_DAEMONIZE=true' \
                    --mount='type=bind,src=/root/spark-apps,dst=/opt/spark/apps,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-data,dst=/opt/spark/data,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-logs,dst=/opt/spark/spark-events,options=rbind:rw' \
                    registry.scality.com/spark/spark-container:3.5.2 spark-master \
                    ./entrypoint.sh master
                    
                    # pause
                    $container_command t kill -a spark-master
                    sleep 1
                    # stop tasks
                    $container_command t rm spark-master
                    sleep 1
                    # resume task
                    $container_command t start -d spark-master
                fi
                
                if [ -n "$local_worker" ] ; then
                    test "$(grep -qw spark-worker /etc/hosts ; echo $?)" == 0 || \
                    echo "$local_worker spark-worker" >> /etc/hosts
                    
                    echo "Running worker here"
                    # remove if exists, throw error if running
                    c=$($container_command c ls |grep -w spark-worker)
                    echo "Checking if the container aleady exists"
                    test -n "$c" && \
                    $container_command c rm spark-worker
                    
                    echo "Creating /tmp/spark-worker directory"
                    mkdir -p /tmp/spark-worker
                    
                    echo "
                    $container_command run -d --net-host \
                    --mount='type=bind,src=/root/spark-apps,dst=/opt/spark/apps,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-data,dst=/opt/spark/data,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-logs,dst=/opt/spark/spark-events,options=rbind:rw' \
                    --mount='type=bind,src=/etc/scality/node/,dst=/etc/scality/node/,options=rbind:r' \
                    --env='SPARK_NO_DAEMONIZE=true' \
                    registry.scality.com/spark/spark-container:3.5.2 spark-worker \
                    ./entrypoint.sh worker
                    " |tr -s ' '
                    $container_command run -d --net-host \
                    --mount='type=bind,src=/root/spark-apps,dst=/opt/spark/apps,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-data,dst=/opt/spark/data,options=rbind:rw' \
                    --mount='type=bind,src=/root/spark-logs,dst=/opt/spark/spark-events,options=rbind:rw' \
                    --mount='type=bind,src=/etc/scality/node/,dst=/etc/scality/node/,options=rbind:r' \
                    --env='SPARK_NO_DAEMONIZE=true' \
                    registry.scality.com/spark/spark-container:3.5.2 spark-worker \
                    ./entrypoint.sh worker
                   
                    # pause
                    $container_command t kill -a spark-worker
                    sleep 1
                    # stop tasks
                    $container_command t rm spark-worker
                    sleep 1
                    # resume task
                    $container_command t start -d spark-worker
                fi
            ;;
        esac
    ;;
    stop) echo "Stopping Spark node"
        case $container_command
            in
            docker) echo "Stopping using $container_command"
                if [ -n "$local_master" ] ; then
                    echo "Stopping master here"
                    $container_command stop spark-master
                fi
                
                if [ -n "$local_worker" ] ; then
                    echo "Stopping worker here"
                    $container_command stop spark-worker
                fi
            ;;
            ctr) echo "Stopping using $container_command"
                # Remember, ctr cannot run in daemon with rm argument like docker
                
                if [ -n "$local_master" ] ; then
                    echo "Stopping master here"
                    $container_command t kill --signal 9 -a spark-master
                    sleep 1
                    $container_command c rm spark-master
                fi
                
                if [ -n "$local_worker" ] ; then
                    echo "Stoping worker here"
                    $container_command t kill --signal 9  -a spark-worker
                    sleep 1
                    $container_command c rm spark-worker
                fi
            ;;
        esac
    ;;
    status) echo "Status of Spark node"
        case $container_command
            in
            docker) $container_command ps |grep spark
            ;;
            ctr) $container_command c ls
                $container_command t ls
            ;;
        esac
    ;;
    *) echo "Usage: $0 <start|stop|status>"
        exit 1
    ;;
esac