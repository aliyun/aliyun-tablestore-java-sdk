#!/bin/bash
# Usage: ./protoc.sh [protoc_path]
# Need to specify the path to the protoc executable file
# Protocol buffer version requirement: 4.28.2

DIR=$(cd $(dirname $0); pwd)
cd ${DIR}


# Define the default protoc executable file path
DEFAULT_PROTOC_PATH="protoc"

# Check if the parameter is provided, if not, use the default value.
if [ -z "$1" ]; then
    PROTOC_PATH="$DEFAULT_PROTOC_PATH"
else
    PROTOC_PATH="$1"
fi

# Check if this path is valid
if [ ! -x "$PROTOC_PATH" ]; then
    echo "Error: '$PROTOC_PATH' is not a valid executable file."
    exit 1
fi

# Output the version information of protoc
echo "Running protoc from: $PROTOC_PATH"
"$PROTOC_PATH" --version

# List of proto files
proto_files=($(find ./src/main/java -type f -name "*.proto"))

build_folder='build-protoc'
mkdir -p "$build_folder"

for proto_file in "${proto_files[@]}"; do
    cp "$proto_file" "$build_folder"
done

cd "$build_folder"

gen_folder='gen'
mkdir -p "$gen_folder"
for proto_file in "${proto_files[@]}"; do
    proto_base=$(basename "$proto_file")

    command="$PROTOC_PATH $proto_base --java_out=./$gen_folder"
    echo "run: $command"
    $command
    if [ $? -ne 0 ]; then
        echo "run command failed: $command"
        exit 1
    fi
done

find . -type f -name "*.java" -exec sed -i '' 's/com\.google\.protobuf/com.aliyun.ots.thirdparty.com.google.protobuf/g' {} \;

rsync -av $gen_folder/ ../src/main/java/

cd ..
rm -r "$build_folder"

echo "all commands done."
