#!/bin/bash
set -e

# Script to publish Rust crates to crates.io
# Publishes in dependency order to ensure dependencies are available

# Add cargo to PATH if installed via rustup
export PATH="$HOME/.cargo/bin:$PATH"

# Colors
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Publishing order (dependencies first)
PUBLISH_ORDER=(
    "types"
    "config"
    "collector"
    "storage"
    "processor"
    "analyzer"
    "decision"
    "actuator"
    "integrations"
    "api"
    "api-rest"
    "api-grpc"
    "api-tests"
    "cli"
    "llm-optimizer"
)

DRY_RUN=${DRY_RUN:-true}
SLEEP_TIME=15 # Sleep between publishes to allow crates.io to index

echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  LLM Auto Optimizer - Crate Publishing Script${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo ""

# Check if we're in dry-run mode
if [ "$DRY_RUN" = "true" ]; then
    echo -e "${YELLOW}🔍 Running in DRY-RUN mode (no actual publishing)${NC}"
    echo -e "${YELLOW}   To publish for real, run: DRY_RUN=false $0${NC}"
else
    echo -e "${RED}⚠️  LIVE MODE - Will publish to crates.io!${NC}"
    echo -e "${YELLOW}   Press Ctrl+C within 5 seconds to cancel...${NC}"
    sleep 5
fi
echo ""

# Check if logged in to crates.io
if ! cargo login --help &> /dev/null; then
    echo -e "${RED}❌ cargo login not available${NC}"
    exit 1
fi

# Check if CARGO_REGISTRY_TOKEN is set (for CI/CD)
if [ -z "$CARGO_REGISTRY_TOKEN" ] && [ "$DRY_RUN" = "false" ]; then
    echo -e "${YELLOW}⚠️  CARGO_REGISTRY_TOKEN not set. Make sure you're logged in with 'cargo login'${NC}"
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        exit 1
    fi
fi

# Build all crates first
echo -e "${BLUE}📦 Building all crates...${NC}"
if ! cargo build --workspace --all-targets; then
    echo -e "${RED}❌ Build failed!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Build successful${NC}"
echo ""

# Run tests
echo -e "${BLUE}🧪 Running tests...${NC}"
if ! cargo test --workspace --all-targets; then
    echo -e "${RED}❌ Tests failed!${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Tests passed${NC}"
echo ""

# Publish each crate in order
echo -e "${BLUE}📤 Publishing crates...${NC}"
echo ""

PUBLISHED_COUNT=0
SKIPPED_COUNT=0
FAILED_COUNT=0

for crate in "${PUBLISH_ORDER[@]}"; do
    CRATE_PATH="crates/$crate"
    CRATE_NAME="llm-optimizer-$crate"

    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"
    echo -e "${BLUE}Processing: ${CRATE_NAME}${NC}"
    echo -e "${BLUE}━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━${NC}"

    if [ ! -d "$CRATE_PATH" ]; then
        echo -e "${RED}❌ Directory not found: $CRATE_PATH${NC}"
        ((FAILED_COUNT++))
        continue
    fi

    cd "$CRATE_PATH"

    # Check if already published
    VERSION=$(cargo metadata --no-deps --format-version 1 | grep -o '"version":"[^"]*"' | head -1 | cut -d'"' -f4)
    echo -e "  Version: ${YELLOW}$VERSION${NC}"

    # Dry run
    echo -e "  Running publish dry-run..."
    if ! cargo publish --dry-run; then
        echo -e "${RED}❌ Dry-run failed for $CRATE_NAME${NC}"
        cd - > /dev/null
        ((FAILED_COUNT++))
        continue
    fi

    if [ "$DRY_RUN" = "true" ]; then
        echo -e "${GREEN}✓ Dry-run successful (not published)${NC}"
        ((SKIPPED_COUNT++))
    else
        echo -e "  Publishing to crates.io..."
        if cargo publish; then
            echo -e "${GREEN}✓ Successfully published $CRATE_NAME@$VERSION${NC}"
            ((PUBLISHED_COUNT++))

            # Sleep to allow crates.io to index
            if [ "$crate" != "${PUBLISH_ORDER[-1]}" ]; then
                echo -e "${YELLOW}  Waiting ${SLEEP_TIME}s for crates.io to index...${NC}"
                sleep $SLEEP_TIME
            fi
        else
            echo -e "${RED}❌ Failed to publish $CRATE_NAME${NC}"
            ((FAILED_COUNT++))
        fi
    fi

    cd - > /dev/null
    echo ""
done

# Summary
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"
echo -e "${BLUE}  Publishing Summary${NC}"
echo -e "${BLUE}════════════════════════════════════════════════════════════════${NC}"

if [ "$DRY_RUN" = "true" ]; then
    echo -e "${GREEN}✓ Validated: $SKIPPED_COUNT crates${NC}"
else
    echo -e "${GREEN}✓ Published: $PUBLISHED_COUNT crates${NC}"
fi

if [ $FAILED_COUNT -gt 0 ]; then
    echo -e "${RED}❌ Failed: $FAILED_COUNT crates${NC}"
    exit 1
fi

echo ""
if [ "$DRY_RUN" = "true" ]; then
    echo -e "${GREEN}🎉 All crates validated successfully!${NC}"
    echo -e "${YELLOW}   Run 'DRY_RUN=false $0' to publish for real${NC}"
else
    echo -e "${GREEN}🎉 All crates published successfully!${NC}"
    echo -e "${GREEN}   View at: https://crates.io/crates/llm-optimizer${NC}"
fi
