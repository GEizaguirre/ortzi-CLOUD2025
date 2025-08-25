#!/bin/bash

# Define the input file and output file
input_file="terasort-20m"
output_file="terasort-240m"

# Define the number of times to concatenate the input file
N=12

# Loop N times and concatenate the input file to the output file
for ((i=1; i<=N; i++))
do
    cat "$input_file" >> "$output_file"
done

echo "File concatenation complete!"