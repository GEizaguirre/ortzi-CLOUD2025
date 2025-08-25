#!/bin/bash
python -m grpc_tools.protoc -I. --python_out=. --pyi_out=. --grpc_python_out=. ./services.proto

sed -i 's/services_pb2 as services__pb2/ortzi.proto.services_pb2 as services__pb2/' services_pb2_grpc.py