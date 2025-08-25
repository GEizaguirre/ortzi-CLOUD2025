#!/bin/bash

data_dir=$1
mkdir -p ${data_dir}
scale=$2

# generate data
./dsdgen -scale ${scale} -dir ${data_dir}

# num_chunks=10
# for i in $(seq 1 ${num_chunks}) 
# do
#     ./dsdgen -scale ${scale} -dir ${data_dir} -terminate n -parallel ${num_chunks} -child ${i}
# done
