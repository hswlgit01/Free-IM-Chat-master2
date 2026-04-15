#!/bin/bash

# 定义颜色
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# 项目根目录
PROJECT_ROOT=$(dirname "$(readlink -f "$0")")
DOCKER_FILE="$PROJECT_ROOT/Dockerfile.build"

echo -e "${BLUE}Building Free-IM-Chat...${NC}"

# 检查Docker是否安装
if ! command -v docker &> /dev/null; then
    echo -e "${RED}Docker is not installed. Please install Docker first.${NC}"
    exit 1
fi

# 创建输出目录
mkdir -p "$PROJECT_ROOT/_output/bin"

# 构建Docker镜像
echo -e "${BLUE}Building Docker image...${NC}"
docker build -t free-im-chat-builder -f "$DOCKER_FILE" "$PROJECT_ROOT"

# 在Docker中编译项目
echo -e "${GREEN}Compiling Free-IM-Chat components...${NC}"
docker run --rm \
    -v "$PROJECT_ROOT:/build/src" \
    -w /build/src \
    free-im-chat-builder \
    bash -c "go build -o _output/bin/admin-rpc cmd/rpc/admin-rpc/main.go && \
             go build -o _output/bin/chat-rpc cmd/rpc/chat-rpc/main.go && \
             go build -o _output/bin/admin-api cmd/api/admin-api/main.go && \
             go build -o _output/bin/chat-api cmd/api/chat-api/main.go"

# 检查编译结果
COMPONENTS=("admin-rpc" "chat-rpc" "admin-api" "chat-api")
BUILD_SUCCESS=true

for component in "${COMPONENTS[@]}"; do
    if [ ! -f "$PROJECT_ROOT/_output/bin/$component" ]; then
        echo -e "${RED}Failed to build $component${NC}"
        BUILD_SUCCESS=false
    fi
done

if $BUILD_SUCCESS; then
    echo -e "${GREEN}All components built successfully!${NC}"
    echo -e "${GREEN}Output directory: ${PROJECT_ROOT}/_output/bin/${NC}"
else
    echo -e "${RED}Some components failed to build.${NC}"
    exit 1
fi