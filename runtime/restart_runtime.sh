# !/bin/bash

function display_help {
    echo "Usage: $0 [backend] [runtime_name] [tag]"
    echo
    echo "Arguments:"
    echo "  backend        The backend to use (aws_lambda or aws_ec2)"
    echo "  runtime_name   The name of the runtime (default is ortzi)"
    echo "  tag            The tag for the runtime (default is 1)"
    echo
    echo "Example:"
    echo "  $0 aws_lambda ortzi 1"
    exit 1
}

if [ "$1" == "--help" ] || [ "$1" == "-h" ]; then
    display_help
fi

if [ $# -lt 2 ]; then
    display_help
fi

backend="$1"

if [ "$backend" != "aws_lambda" ] && [ "$backend" != "aws_ec2" ] && [ "$backend" != "k8s" ]; then
    echo "Error: Invalid backend. Must be 'aws_lambda' or 'aws_ec2'."
    display_help
fi
runtime_name="$2"
tag="${3:-1}"

if [ -z "$backend" ]; then
    backend="aws_lambda"
fi

if [ -z "$runtime_name" ]; then
    runtime_name="ortzi"
fi

if [ -z "$tag" ]; then
    tag="1"
fi


zip -r ortzi.zip . \
    -x "data/*" \
    -x ".git/*" \
    -x "tests/*" \
    -x "ortzi.egg-info/*" \
    -x "runtime/*" \
    -x "venv/*" \
    -x "ortzi.zip" \
    -x "test*" \
    -x ".ipynb*" \
    -x ".venv/*" \
    -x "*__pycache__*" \
    -x "*.egg-info/*" \
    -x "*.egg" \
    -x "*.whl" \
    -x ".DS_Store" \
    -x ".vscode/*" \
    -x "build/*" \
    -x "img/*" \
    -x "aws_s3_tests/*"

if [ "$backend" == "aws_lambda" ]; then
    echo "Building AWS Lambda runtime"

    python3 runtime/set_lithops_config.py

    lithops runtime delete -b aws_lambda $runtime_name:$tag --debug
    lithops runtime build -f runtime/Dockerfile -b aws_lambda $runtime_name:$tag --debug
    lithops runtime update -b aws_lambda $runtime_name:$tag --debug
elif [ "$backend" == "k8s" ]; then
    lithops clean
    echo "Building docker runtime"
    docker_username=$(python3 -c "import yaml; print(yaml.safe_load(open('$HOME/.lithops/config'))['k8s']['docker_user'])")
    echo "Docker username: $docker_username"
    lithops runtime build -f runtime/Dockerfile_k8s -b k8s $docker_username/$runtime_name:$tag
else
    echo "Building AWS EC2 runtime"
    runtime/build_runtime_ec2.sh $runtime_name $tag
fi

rm ortzi.zip
