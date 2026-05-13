#!/bin/bash
# Usage: ./protoc.sh [protoc_path]
# Need to specify the path to the protoc executable file
# Protocol buffer version requirement: 4.28.2

DIR="$(cd "$(dirname "$0")" && pwd)"
cd "${DIR}"


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

build_folder='build-protoc'
mkdir -p "$build_folder"
proto_list_file="${build_folder}/proto_files.list"
: > "$proto_list_file"

find ./src/main/java -type f -name "*.proto" -print0 | while IFS= read -r -d '' proto_file; do
    rel_proto_path="${proto_file#./src/main/java/}"
    dest_dir="${build_folder}/$(dirname "$rel_proto_path")"
    mkdir -p "$dest_dir"
    cp "$proto_file" "$dest_dir/"
    printf '%s\n' "$rel_proto_path" >> "$proto_list_file"
done

cd "$build_folder"

gen_folder='gen'
mkdir -p "$gen_folder"

# Collect all unique directories containing proto files as proto_paths
proto_paths=()
while IFS= read -r proto_base; do
    [ -n "$proto_base" ] || continue
    dir=$(dirname "$proto_base")
    # Add directory if not already in the list
    found=0
    for p in "${proto_paths[@]}"; do
        if [ "$p" = "$dir" ]; then
            found=1
            break
        fi
    done
    if [ $found -eq 0 ]; then
        proto_paths+=("$dir")
    fi
done < "$(basename "$proto_list_file")"

# Build proto_path arguments
proto_path_args=("--proto_path=.")
for p in "${proto_paths[@]}"; do
    proto_path_args+=("--proto_path=$p")
done

while IFS= read -r proto_base; do
    [ -n "$proto_base" ] || continue

    echo "run: \"$PROTOC_PATH\" ${proto_path_args[*]} --java_out=./$gen_folder \"$proto_base\""
    "$PROTOC_PATH" "${proto_path_args[@]}" "--java_out=./$gen_folder" "$proto_base"
    if [ $? -ne 0 ]; then
        echo "run command failed: $proto_base"
        exit 1
    fi
done < "$(basename "$proto_list_file")"

find . -type f -name "*.java" -print0 | while IFS= read -r -d '' generated_java; do
    tmp_file="${generated_java}.tmp"
    sed 's/com\.google\.protobuf/com.aliyun.ots.thirdparty.com.google.protobuf/g' "$generated_java" > "$tmp_file" && cat "$tmp_file" > "$generated_java" && rm -f "$tmp_file"
done

rsync -avc "$gen_folder"/ ../src/main/java/

cd ..
rm -r "$build_folder"

echo "all commands done."
