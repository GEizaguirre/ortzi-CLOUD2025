# !/bin/bash
RUNTIME=$1
VERSION=$2

RUNTIME_FILE="runtime/ec2.Dockerfile"

LITHOPS_VERSION=$(pip show lithops | awk '/Version/ {print $2}')

echo "Lithops version at local machine: $LITHOPS_VERSION"

if [[ $LITHOPS_VERSION == *.dev0 ]]; then
    LITHOPS_VERSION=$(pip install --quiet --disable-pip-version-check lithops==random 2>&1 | \
                      grep -oP '(?<=from versions: )[^)]*' | tr ', ' '\n' | \
                      grep -v dev0 | sort -V | tail -n1)
fi

echo "Installing Lithops version: $LITHOPS_VERSION"

aws ecr-public get-login-password --region us-east-1 | docker login --username AWS --password-stdin public.ecr.aws

alias=$(aws ecr-public describe-registries --query "registries[0].aliases[0].name" --output text)

existing_repo=$(aws ecr-public describe-repositories --query "repositories[?repositoryName=='$RUNTIME'].repositoryName" --output text)

if [ "$existing_repo" == "$RUNTIME" ]; then
    echo "Repository $RUNTIME already exists. Deleting it."
    aws ecr-public delete-repository --repository-name $RUNTIME --force
fi

echo "Creating ECR repository: $RUNTIME for user $alias"

aws ecr-public create-repository --repository-name $RUNTIME

echo "Building Docker image public.ecr.aws/$alias/$RUNTIME:$VERSION"

docker build --no-cache --platform=linux/amd64 --build-arg LITHOPS_VERSION=$LITHOPS_VERSION -t public.ecr.aws/$alias/$RUNTIME:$VERSION -f $RUNTIME_FILE .

echo "Pushing Docker image to ECR"

docker push public.ecr.aws/$alias/$RUNTIME:$VERSION

