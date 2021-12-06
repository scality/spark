#!/bin/bash

echo "WARNING: THIS TOOL IS CURRENTLY SETUP TO SUGGEST TUNING VALUES FOR SINGLE RING S3C ENVIRONMENTS"

echo -e "\nHow many S3C endpoints run a spark worker?"
read Endpoints

echo -e "\nHow many cores does each endpoint have?"
read Endpoint_Cores

echo -e "\nCheck the output of free -h on each endpoint running a spark worker."
echo "Find which endpoint has the least amount of memory in the Available column (NOT the Free column)."
echo -e "\nWhat is the least amount of available memory available to a spark worker?"
read AvailableMemInGB

echo -e "\nIs the platform going to be idle during the scan and or cleanup phases? [yes|no]"
read Platform_Idle

# The IO will taper off around 5 concurrent executors. Optimal is suggested to be 3 per worker.
# Subtract one additional worker so the Application Master runs only 2 executors.
spark_instances=$(echo $(( ${Endpoints} * 3 - 1 )) )


#idle=${Platform_Idle,,}
idle=$(echo ${Platform_Idle} | tr '[:upper:]' '[:lower:]')
if [ "${idle}" = "n" ] || [ "${idle}" = "no" ] ; then
    # Because the system is not idle we only consider 1/2 the available memory
    # Divide by 6 to find memory for each of the three executors
    spark_mem=$(echo $((${AvailableMemInGB} / 6)) )
    # Because the system is not idle divide the total cores by 2.
    # Next subtract one additional core for breathing room
    # Then divide by three to find the cores per executor
    spark_cores=$(echo $(( (${Endpoint_Cores} / 2 - 1) / 3 )) )
elif [ "${idle}" = "y" ] || [ "${idle}" = "yes" ] ; then
    # Because the system is idle we consider all available memory.
    # Next subtract 4GB for vm.min_free_kbytes plus breathing room
    # Then divide by three to find memory per executor
    spark_mem=$(echo $(( ((${AvailableMemInGB} - 4 ) / 3 ) )) )
    # Because the system is idle consider all available cores.
    # Next subtract 7 cores for the ring, biziod and system to have adequate resources
    # Then divide by three to find the cores per executor
    spark_cores=$(echo $(( (${Endpoint_Cores} - 7) / 3 )) )
else
    echo -e "\n---ERROR: Whether platform is idle is not yes or no.---\n"
    exit 1
fi

echo -e "\nSuggested tuning based on input provided is to set the configuration of the spark executors to:\n"

echo -e "\n-------------------------------------------\n"
echo "spark.executor.instances: ${spark_instances}"
echo "spark.executor.cores: ${spark_cores}"
echo "spark.executor.memory: ${spark_mem}"
echo -e "\n-------------------------------------------\n"