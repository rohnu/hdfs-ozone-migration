#!/usr/bin/env bash
#
# HMS Mirror DistCP Runner Script with Table-Level Chunking
# Executes distcp operations from HMS Mirror reports with table-level checkpointing and resume capability.
#

set -e

# ==============================================================================
# Configuration
# ==============================================================================

DEFAULT_CONFIG_FILE="./runnerconfig.ini"
DEFAULT_QUEUE_NAME="default"
DEFAULT_DISTCP_BANDWIDTH_MB="100"
DEFAULT_DISTCP_NUM_MAPS="20"
DEFAULT_DISTCP_MEMORY_MB="4096"
DEFAULT_HCFS_BASE_DIR="/tmp/hms-mirror-distcp"
DEFAULT_CHECKPOINT_DIR="/tmp/hms-mirror-checkpoint"
DEFAULT_KEYTAB_PATH=""
DEFAULT_PRINCIPAL=""
DEFAULT_CHUNK_SIZE="100"  # Default number of tables per chunk

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# ==============================================================================
# Functions
# ==============================================================================

log_info() {
    echo -e "${BLUE}[INFO]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $(date '+%Y-%m-%d %H:%M:%S') - $1"
}

load_config() {
    local CONFIG_FILE="$1"
    
    if [ ! -f "$CONFIG_FILE" ]; then
        log_warning "Config file not found: $CONFIG_FILE"
        log_info "Using default values"
        return 1
    fi
    
    log_info "Loading configuration from: $CONFIG_FILE"
    
    while IFS='=' read -r key value; do
        [[ "$key" =~ ^#.*$ ]] && continue
        [[ -z "$key" ]] && continue
        
        key=$(echo "$key" | xargs)
        value=$(echo "$value" | xargs | sed -e 's/^"//' -e 's/"$//')
        
        case "$key" in
            QUEUE_NAME) QUEUE_NAME="$value" ;;
            DISTCP_BANDWIDTH_MB) DISTCP_BANDWIDTH_MB="$value" ;;
            DISTCP_NUM_MAPS) DISTCP_NUM_MAPS="$value" ;;
            DISTCP_MEMORY_MB) DISTCP_MEMORY_MB="$value" ;;
            HCFS_BASE_DIR) HCFS_BASE_DIR="$value" ;;
            CHECKPOINT_DIR) CHECKPOINT_DIR="$value" ;;
            KEYTAB_PATH) KEYTAB_PATH="$value" ;;
            PRINCIPAL) PRINCIPAL="$value" ;;
            CHUNK_SIZE) CHUNK_SIZE="$value" ;;
        esac
    done < "$CONFIG_FILE"
    
    log_success "Configuration loaded successfully"
    return 0
}

check_kerberos() {
    log_info "Checking Kerberos authentication..."
    
    if ! command -v klist &> /dev/null; then
        log_error "klist command not found"
        return 1
    fi
    
    if klist -s 2>/dev/null; then
        TICKET_INFO=$(klist 2>/dev/null | grep "Default principal" | head -1)
        log_success "Valid Kerberos ticket found: $TICKET_INFO"
        return 0
    else
        log_warning "No valid Kerberos ticket found"
        return 1
    fi
}

kinit_with_keytab() {
    local KEYTAB="$1"
    local PRINCIPAL="$2"
    
    if [ -z "$KEYTAB" ] || [ -z "$PRINCIPAL" ]; then
        log_error "KEYTAB_PATH and PRINCIPAL required"
        return 1
    fi
    
    KEYTAB="${KEYTAB/#\~/$HOME}"
    
    if [ ! -f "$KEYTAB" ]; then
        log_error "Keytab file not found: $KEYTAB"
        return 1
    fi
    
    log_info "Authenticating with Kerberos..."
    log_info "  Keytab: $KEYTAB"
    log_info "  Principal: $PRINCIPAL"
    
    if kinit -kt "$KEYTAB" "$PRINCIPAL" 2>/dev/null; then
        log_success "Kerberos authentication successful"
        return 0
    else
        log_error "Kerberos authentication failed"
        return 1
    fi
}

ensure_kerberos_auth() {
    if check_kerberos; then
        return 0
    fi
    
    log_warning "Attempting to authenticate with keytab..."
    
    if kinit_with_keytab "$KEYTAB_PATH" "$PRINCIPAL"; then
        return 0
    else
        log_error "Failed to obtain Kerberos credentials"
        return 1
    fi
}

extract_target_path_from_script() {
    local SCRIPT_FILE="$1"
    
    # Extract the target path from the distcp command in the script
    # Looking for pattern: hadoop distcp ... <target_path>
    local TARGET=$(grep -oP "hadoop distcp.*\K\s+\S+$" "$SCRIPT_FILE" | xargs)
    
    if [ -z "$TARGET" ]; then
        log_error "Could not extract target path from script: $SCRIPT_FILE"
        return 1
    fi
    
    echo "$TARGET"
}

split_source_file_into_chunks() {
    local SOURCE_FILE="$1"
    local CHUNK_SIZE="$2"
    local OUTPUT_DIR="$3"
    local BASE_NAME=$(basename "$SOURCE_FILE" .txt)
    
    # All logging goes to stderr to avoid contaminating stdout
    log_info "Chunking source file: $(basename "$SOURCE_FILE")" >&2
    log_info "  Chunk size: $CHUNK_SIZE tables per chunk" >&2
    log_info "  Output directory: $OUTPUT_DIR" >&2
    
    mkdir -p "$OUTPUT_DIR"
    
    # Count total tables in source
    local TOTAL_TABLES=$(wc -l < "$SOURCE_FILE")
    local EXPECTED_CHUNKS=$(( (TOTAL_TABLES + CHUNK_SIZE - 1) / CHUNK_SIZE ))
    
    # Check if chunks already exist from a previous run
    local EXISTING_CHUNKS=$(find "$OUTPUT_DIR" -name "${BASE_NAME}_chunk_*.txt" -type f 2>/dev/null | wc -l)
    
    if [ "$EXISTING_CHUNKS" -gt 0 ]; then
        # Verify chunks are consistent with current chunk size
        # If chunk count doesn't match, recreate chunks
        if [ "$EXISTING_CHUNKS" -eq "$EXPECTED_CHUNKS" ]; then
            log_info "  Found $EXISTING_CHUNKS existing chunks (matches expected), reusing them" >&2
            log_info "  Cleaning only filtered files from previous run" >&2
            rm -f "${OUTPUT_DIR}/${BASE_NAME}_chunk_"*.filtered 2>/dev/null
            
            # Return existing chunks
            find "$OUTPUT_DIR" -name "${BASE_NAME}_chunk_*.txt" -type f | sort
            return 0
        else
            log_warning "  Found $EXISTING_CHUNKS chunks but expected $EXPECTED_CHUNKS (chunk size changed?)" >&2
            log_info "  Recreating chunks with current chunk size" >&2
            rm -f "${OUTPUT_DIR}/${BASE_NAME}_chunk_"*.txt 2>/dev/null
            rm -f "${OUTPUT_DIR}/${BASE_NAME}_chunk_"*.filtered 2>/dev/null
        fi
    fi
    
    log_info "  Creating new chunks" >&2
    log_info "  Total tables: $TOTAL_TABLES" >&2
    log_info "  Number of chunks: $EXPECTED_CHUNKS" >&2
    
    # Split the file into chunks
    split -l "$CHUNK_SIZE" -d -a 4 "$SOURCE_FILE" "${OUTPUT_DIR}/${BASE_NAME}_chunk_"
    
    # Rename chunks to have .txt extension and output to stdout
    for chunk in "${OUTPUT_DIR}/${BASE_NAME}_chunk_"*; do
        if [[ ! "$chunk" == *.txt ]]; then
            mv "$chunk" "${chunk}.txt"
            echo "${chunk}.txt"
        else
            echo "$chunk"
        fi
    done | sort
}

get_table_name_from_path() {
    local TABLE_PATH="$1"
    # Extract table name from path (last component)
    basename "$TABLE_PATH"
}

execute_distcp_for_chunk() {
    local CHUNK_FILE="$1"
    local TARGET_PATH="$2"
    local DB_NAME="$3"
    local CHUNK_NUMBER="$4"
    local TOTAL_CHUNKS="$5"
    
    local CHUNK_NAME=$(basename "$CHUNK_FILE")
    
    log_info "Processing chunk $CHUNK_NUMBER/$TOTAL_CHUNKS: $CHUNK_NAME"
    
    # Safely get table count
    local CHUNK_TABLE_COUNT=0
    if [ -f "$CHUNK_FILE" ]; then
        CHUNK_TABLE_COUNT=$(wc -l < "$CHUNK_FILE" 2>/dev/null || echo "0")
    fi
    log_info "  Tables in chunk: $CHUNK_TABLE_COUNT"
    
    # Create unique HCFS path for this chunk (remove .txt and .filtered extensions)
    local CHUNK_BASE=$(basename "$CHUNK_FILE" .filtered)
    CHUNK_BASE=$(basename "$CHUNK_BASE" .txt)
    local CHUNK_HCFS_DIR="${HCFS_BASE_DIR}/${DB_NAME}/${CHUNK_BASE}"
    
    log_info "  Creating HCFS directory: $CHUNK_HCFS_DIR"
    hdfs dfs -mkdir -p "$CHUNK_HCFS_DIR" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        log_error "Failed to create HCFS directory: $CHUNK_HCFS_DIR"
        return 1
    fi
    
    local CHUNK_FILENAME=$(basename "$CHUNK_FILE")
    local CHUNK_HDFS_PATH="${CHUNK_HCFS_DIR}/${CHUNK_FILENAME}"
    
    log_info "  Copying chunk file to HDFS: $CHUNK_HDFS_PATH"
    hdfs dfs -copyFromLocal -f "$CHUNK_FILE" "$CHUNK_HDFS_PATH" >> "$LOG_FILE" 2>&1
    if [ $? -ne 0 ]; then
        log_error "Failed to copy chunk file to HDFS"
        return 1
    fi
    
    log_info "  Running distcp for chunk..."
    log_info "  Command: hadoop distcp ${DISTCP_OPTS} -skipcrccheck -f ${CHUNK_HDFS_PATH} ${TARGET_PATH}"
    
    hadoop distcp ${DISTCP_OPTS} -skipcrccheck -f "${CHUNK_HDFS_PATH}" "${TARGET_PATH}" >> "$LOG_FILE" 2>&1
    local DISTCP_RESULT=$?
    
    if [ $DISTCP_RESULT -eq 0 ]; then
        log_success "  Chunk completed successfully"
        return 0
    else
        log_error "  Chunk failed (exit code: $DISTCP_RESULT)"
        return 1
    fi
}

process_database_with_chunking() {
    local DB_DIR="$1"
    local DB_NAME=$(basename "$DB_DIR" | sed "s/_${MODE}//")
    
    log_info "Processing database: $DB_NAME"
    
    # Find the distcp script
    local DISTCP_SCRIPT=$(find "$DB_DIR" -maxdepth 1 -name "*_distcp_script.sh" | head -1)
    
    if [ -z "$DISTCP_SCRIPT" ]; then
        log_error "No distcp script found in: $DB_DIR"
        return 1
    fi
    
    log_info "Found distcp script: $(basename "$DISTCP_SCRIPT")"
    
    # Extract target path from the script
    local TARGET_PATH=$(extract_target_path_from_script "$DISTCP_SCRIPT")
    if [ -z "$TARGET_PATH" ]; then
        log_error "Failed to extract target path from script"
        return 1
    fi
    
    log_info "Target path: $TARGET_PATH"
    
    # Find all distcp source files
    local SOURCE_FILES=$(find "$DB_DIR" -maxdepth 1 -name "*_distcp_source.txt" | sort)
    
    if [ -z "$SOURCE_FILES" ]; then
        log_error "No distcp source files found in: $DB_DIR"
        return 1
    fi
    
    local SOURCE_FILE_COUNT=$(echo "$SOURCE_FILES" | wc -l)
    log_info "Found $SOURCE_FILE_COUNT distcp source file(s)"
    
    # Create checkpoint directory for this database
    local DB_CHECKPOINT_DIR="${CHECKPOINT_DIR}/tables_${DB_NAME}_${MODE}"
    mkdir -p "$DB_CHECKPOINT_DIR"
    
    local DB_COMPLETED_TABLES="${DB_CHECKPOINT_DIR}/completed_tables.txt"
    local DB_FAILED_TABLES="${DB_CHECKPOINT_DIR}/failed_tables.txt"
    local DB_CHUNKS_DIR="${DB_CHECKPOINT_DIR}/chunks"
    
    # Initialize checkpoint files if they don't exist
    if [ ! -f "$DB_COMPLETED_TABLES" ]; then
        > "$DB_COMPLETED_TABLES"
    fi
    if [ ! -f "$DB_FAILED_TABLES" ]; then
        > "$DB_FAILED_TABLES"
    fi
    
    # Load completed tables
    local COMPLETED_TABLES=()
    if [ -f "$DB_COMPLETED_TABLES" ]; then
        mapfile -t COMPLETED_TABLES < "$DB_COMPLETED_TABLES"
    fi
    
    log_info "Previously completed tables: ${#COMPLETED_TABLES[@]}"
    
    local TOTAL_SUCCESS=0
    local TOTAL_FAILED=0
    
    # Process each source file
    for SOURCE_FILE in $SOURCE_FILES; do
        local SOURCE_BASE=$(basename "$SOURCE_FILE" .txt)
        log_info "Processing source file: $(basename "$SOURCE_FILE")"
        
        local TOTAL_TABLES=$(wc -l < "$SOURCE_FILE")
        log_info "  Total tables in source: $TOTAL_TABLES"
        
        # Check if chunking is needed
        if [ "$TOTAL_TABLES" -le "$CHUNK_SIZE" ]; then
            log_info "  Table count ($TOTAL_TABLES) is within chunk size ($CHUNK_SIZE), processing without chunking"
            
            # Filter out completed tables
            local TEMP_SOURCE="${DB_CHUNKS_DIR}/${SOURCE_BASE}_filtered.txt"
            mkdir -p "$(dirname "$TEMP_SOURCE")"
            
            > "$TEMP_SOURCE"
            while IFS= read -r table_path; do
                local table_name=$(get_table_name_from_path "$table_path")
                if [[ ! " ${COMPLETED_TABLES[@]} " =~ " ${table_name} " ]]; then
                    echo "$table_path" >> "$TEMP_SOURCE"
                fi
            done < "$SOURCE_FILE"
            
            local REMAINING_TABLES=$(wc -l < "$TEMP_SOURCE")
            
            if [ "$REMAINING_TABLES" -eq 0 ]; then
                log_success "  All tables already completed, skipping"
                continue
            fi
            
            log_info "  Tables to process: $REMAINING_TABLES"
            
            if [ "$DRY_RUN" = true ]; then
                log_info "  [DRY RUN] Would process $REMAINING_TABLES tables"
                head -5 "$TEMP_SOURCE" | while IFS= read -r table_path; do
                    local table_name=$(get_table_name_from_path "$table_path")
                    log_info "  [DRY RUN]     • $table_name"
                done
                TOTAL_SUCCESS=$((TOTAL_SUCCESS + REMAINING_TABLES))
                continue
            fi
            
            # Execute distcp for this source file
            if execute_distcp_for_chunk "$TEMP_SOURCE" "$TARGET_PATH" "$DB_NAME" "1" "1"; then
                # Mark all tables as completed
                while IFS= read -r table_path; do
                    local table_name=$(get_table_name_from_path "$table_path")
                    echo "$table_name" >> "$DB_COMPLETED_TABLES"
                    COMPLETED_TABLES+=("$table_name")
                done < "$TEMP_SOURCE"
                TOTAL_SUCCESS=$((TOTAL_SUCCESS + REMAINING_TABLES))
            else
                # Mark all tables as failed
                while IFS= read -r table_path; do
                    local table_name=$(get_table_name_from_path "$table_path")
                    echo "$table_name" >> "$DB_FAILED_TABLES"
                done < "$TEMP_SOURCE"
                TOTAL_FAILED=$((TOTAL_FAILED + REMAINING_TABLES))
                return 1
            fi
            
        else
            log_info "  Chunking required (chunk size: $CHUNK_SIZE)"
            
            # Split source file into chunks - capture output properly
            local CHUNK_FILES=()
            while IFS= read -r chunk; do
                [ -n "$chunk" ] && CHUNK_FILES+=("$chunk")
            done < <(split_source_file_into_chunks "$SOURCE_FILE" "$CHUNK_SIZE" "$DB_CHUNKS_DIR")
            
            local TOTAL_CHUNKS=${#CHUNK_FILES[@]}
            log_info "  Created $TOTAL_CHUNKS chunks"
            
            # Process each chunk
            local CHUNK_NUM=0
            for CHUNK_FILE in "${CHUNK_FILES[@]}"; do
                CHUNK_NUM=$((CHUNK_NUM + 1))
                
                log_info "  Processing chunk $CHUNK_NUM/$TOTAL_CHUNKS"
                
                # Filter out completed tables from chunk
                local TEMP_CHUNK="${CHUNK_FILE}.filtered"
                > "$TEMP_CHUNK"
                
                while IFS= read -r table_path; do
                    local table_name=$(get_table_name_from_path "$table_path")
                    
                    if [[ ! " ${COMPLETED_TABLES[@]} " =~ " ${table_name} " ]]; then
                        echo "$table_path" >> "$TEMP_CHUNK"
                    fi
                done < "$CHUNK_FILE"
                
                local REMAINING_IN_CHUNK=$(wc -l < "$TEMP_CHUNK" 2>/dev/null || echo "0")
                
                if [ "$REMAINING_IN_CHUNK" -eq 0 ]; then
                    log_success "    All tables in chunk already completed, skipping"
                    continue
                fi
                
                log_info "    Tables to process in chunk: $REMAINING_IN_CHUNK"
                
                if [ "$DRY_RUN" = true ]; then
                    log_info "    [DRY RUN] Would process $REMAINING_IN_CHUNK tables"
                    head -3 "$TEMP_CHUNK" | while IFS= read -r table_path; do
                        local table_name=$(get_table_name_from_path "$table_path")
                        log_info "    [DRY RUN]       • $table_name"
                    done
                    TOTAL_SUCCESS=$((TOTAL_SUCCESS + REMAINING_IN_CHUNK))
                    continue
                fi
                
                # Execute distcp for this chunk
                if execute_distcp_for_chunk "$TEMP_CHUNK" "$TARGET_PATH" "$DB_NAME" "$CHUNK_NUM" "$TOTAL_CHUNKS"; then
                    # Mark ONLY the tables that were actually processed (in filtered chunk)
                    while IFS= read -r table_path; do
                        local table_name=$(get_table_name_from_path "$table_path")
                        if [[ ! " ${COMPLETED_TABLES[@]} " =~ " ${table_name} " ]]; then
                            echo "$table_name" >> "$DB_COMPLETED_TABLES"
                            COMPLETED_TABLES+=("$table_name")
                            TOTAL_SUCCESS=$((TOTAL_SUCCESS + 1))
                        fi
                    done < "$TEMP_CHUNK"
                    
                    # Clean up filtered file after successful processing
                    rm -f "$TEMP_CHUNK" 2>/dev/null
                else
                    log_error "    Chunk $CHUNK_NUM failed"
                    
                    # Mark chunk tables as failed
                    while IFS= read -r table_path; do
                        local table_name=$(get_table_name_from_path "$table_path")
                        echo "$table_name" >> "$DB_FAILED_TABLES"
                        TOTAL_FAILED=$((TOTAL_FAILED + 1))
                    done < "$TEMP_CHUNK"
                    
                    echo ""
                    read -p "    Continue with remaining chunks? (yes/no): " CONTINUE
                    if [[ ! "$CONTINUE" =~ ^[Yy]([Ee][Ss])?$ ]]; then
                        log_warning "    Aborting remaining chunks"
                        return 1
                    fi
                fi
            done
        fi
    done
    
    log_info "Database summary for $DB_NAME:"
    log_success "  Successfully migrated: $TOTAL_SUCCESS tables"
    if [ "$TOTAL_FAILED" -gt 0 ]; then
        log_error "  Failed: $TOTAL_FAILED tables"
        return 1
    fi
    
    return 0
}

usage() {
    cat << EOF
Usage: $0 [OPTIONS]

HMS Mirror DistCP Runner with Table-Level Chunking and Checkpointing

Required Arguments:
  --report-dir <path>       HMS Mirror report directory
  --mode <mode>             Migration mode (STORAGE_MIGRATION, SCHEMA_ONLY, etc.)

Optional Arguments:
  --config <file>           Config file (default: ./runnerconfig.ini)
  --queue <name>            YARN queue name
  --bandwidth <MB>          Bandwidth per map in MB
  --maps <num>              Number of mappers
  --memory <MB>             Memory per mapper in MB
  --hcfs-base <path>        HCFS base directory
  --checkpoint-dir <path>   Checkpoint directory
  --chunk-size <num>        Number of tables per chunk (default: 100)
  --keytab <path>           Kerberos keytab path
  --principal <name>        Kerberos principal
  --resume                  Resume from checkpoint
  --dry-run                 Preview mode
  --help                    Show this help

Examples:
  $0 --report-dir ~/.hms-mirror/reports/2026-01-07_10-22-04 --mode STORAGE_MIGRATION
  $0 --report-dir ~/.hms-mirror/reports/2026-01-07_10-22-04 --mode STORAGE_MIGRATION --chunk-size 50
  $0 --report-dir ~/.hms-mirror/reports/2026-01-07_10-22-04 --mode STORAGE_MIGRATION --resume

EOF
    exit 1
}

# ==============================================================================
# Parse Arguments
# ==============================================================================

REPORT_DIR=""
MODE=""
CONFIG_FILE="${DEFAULT_CONFIG_FILE}"
QUEUE_NAME=""
DISTCP_BANDWIDTH_MB=""
DISTCP_NUM_MAPS=""
DISTCP_MEMORY_MB=""
HCFS_BASE_DIR=""
CHECKPOINT_DIR=""
CHUNK_SIZE=""
KEYTAB_PATH=""
PRINCIPAL=""
RESUME=false
DRY_RUN=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --report-dir) REPORT_DIR="$2"; shift 2 ;;
        --mode) MODE="$2"; shift 2 ;;
        --config) CONFIG_FILE="$2"; shift 2 ;;
        --queue) QUEUE_NAME="$2"; shift 2 ;;
        --bandwidth) DISTCP_BANDWIDTH_MB="$2"; shift 2 ;;
        --maps) DISTCP_NUM_MAPS="$2"; shift 2 ;;
        --memory) DISTCP_MEMORY_MB="$2"; shift 2 ;;
        --hcfs-base) HCFS_BASE_DIR="$2"; shift 2 ;;
        --checkpoint-dir) CHECKPOINT_DIR="$2"; shift 2 ;;
        --chunk-size) CHUNK_SIZE="$2"; shift 2 ;;
        --keytab) KEYTAB_PATH="$2"; shift 2 ;;
        --principal) PRINCIPAL="$2"; shift 2 ;;
        --resume) RESUME=true; shift ;;
        --dry-run) DRY_RUN=true; shift ;;
        --help) usage ;;
        *) log_error "Unknown option: $1"; usage ;;
    esac
done

load_config "$CONFIG_FILE"

QUEUE_NAME="${QUEUE_NAME:-${DEFAULT_QUEUE_NAME}}"
DISTCP_BANDWIDTH_MB="${DISTCP_BANDWIDTH_MB:-${DEFAULT_DISTCP_BANDWIDTH_MB}}"
DISTCP_NUM_MAPS="${DISTCP_NUM_MAPS:-${DEFAULT_DISTCP_NUM_MAPS}}"
DISTCP_MEMORY_MB="${DISTCP_MEMORY_MB:-${DEFAULT_DISTCP_MEMORY_MB}}"
HCFS_BASE_DIR="${HCFS_BASE_DIR:-${DEFAULT_HCFS_BASE_DIR}}"
CHECKPOINT_DIR="${CHECKPOINT_DIR:-${DEFAULT_CHECKPOINT_DIR}}"
CHUNK_SIZE="${CHUNK_SIZE:-${DEFAULT_CHUNK_SIZE}}"
KEYTAB_PATH="${KEYTAB_PATH:-${DEFAULT_KEYTAB_PATH}}"
PRINCIPAL="${PRINCIPAL:-${DEFAULT_PRINCIPAL}}"

if [ -z "$REPORT_DIR" ] || [ -z "$MODE" ]; then
    log_error "Missing required arguments"
    usage
fi

REPORT_DIR="${REPORT_DIR/#\~/$HOME}"

if [ ! -d "$REPORT_DIR" ]; then
    log_error "Report directory does not exist: $REPORT_DIR"
    exit 1
fi

# ==============================================================================
# Setup
# ==============================================================================

log_info "======================================================================"
log_info "KERBEROS AUTHENTICATION"
log_info "======================================================================"

if ! ensure_kerberos_auth; then
    log_error "Failed to obtain Kerberos authentication"
    exit 1
fi

echo ""

mkdir -p "$CHECKPOINT_DIR"

if [ ! -d "$CHECKPOINT_DIR" ]; then
    log_error "Failed to create checkpoint directory: $CHECKPOINT_DIR"
    exit 1
fi

log_info "Checkpoint directory: $CHECKPOINT_DIR"

log_info "Creating HCFS base directory: $HCFS_BASE_DIR"
if ! hdfs dfs -mkdir -p "$HCFS_BASE_DIR" 2>/dev/null; then
    log_warning "Could not create HCFS directory"
fi

CHECKPOINT_FILE="${CHECKPOINT_DIR}/checkpoint_$(basename ${REPORT_DIR})_${MODE}.txt"
COMPLETED_FILE="${CHECKPOINT_DIR}/completed_$(basename ${REPORT_DIR})_${MODE}.txt"
FAILED_FILE="${CHECKPOINT_DIR}/failed_$(basename ${REPORT_DIR})_${MODE}.txt"
LOG_FILE="${CHECKPOINT_DIR}/distcp_$(basename ${REPORT_DIR})_${MODE}_$(date +%Y%m%d_%H%M%S).log"

DISTCP_OPTS=""
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.job.queuename=${QUEUE_NAME}"
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.map.memory.mb=${DISTCP_MEMORY_MB}"
DISTCP_OPTS="$DISTCP_OPTS -Dmapreduce.map.java.opts=-Xmx$((DISTCP_MEMORY_MB * 80 / 100))m"
DISTCP_OPTS="$DISTCP_OPTS -bandwidth ${DISTCP_BANDWIDTH_MB}"
DISTCP_OPTS="$DISTCP_OPTS -m ${DISTCP_NUM_MAPS}"

export DISTCP_OPTS
export HCFS_BASE_DIR

# ==============================================================================
# Display Configuration
# ==============================================================================

log_info "======================================================================"
log_info "HMS MIRROR DISTCP RUNNER WITH CHUNKING"
log_info "======================================================================"
log_info "Config File: $CONFIG_FILE"
log_info "Report Directory: $REPORT_DIR"
log_info "Mode: $MODE"
log_info "Queue Name: $QUEUE_NAME"
log_info "Bandwidth per Map: ${DISTCP_BANDWIDTH_MB} MB"
log_info "Number of Maps: $DISTCP_NUM_MAPS"
log_info "Memory per Map: ${DISTCP_MEMORY_MB} MB"
log_info "HCFS Base Dir: $HCFS_BASE_DIR"
log_info "Checkpoint Dir: $CHECKPOINT_DIR"
log_info "Chunk Size: $CHUNK_SIZE tables per chunk"
log_info "Resume Mode: $RESUME"
log_info "Dry Run: $DRY_RUN"
if [ -n "$KEYTAB_PATH" ]; then
    log_info "Keytab: $KEYTAB_PATH"
fi
if [ -n "$PRINCIPAL" ]; then
    log_info "Principal: $PRINCIPAL"
fi
log_info "======================================================================"

# ==============================================================================
# Find Matching Directories
# ==============================================================================

log_info "Searching for directories matching pattern: *_${MODE}"

mapfile -t ALL_DIRS < <(find "$REPORT_DIR" -maxdepth 1 -type d -name "*_${MODE}" | sort)

if [ ${#ALL_DIRS[@]} -eq 0 ]; then
    log_error "No directories found matching pattern: *_${MODE}"
    exit 1
fi

log_info "Found ${#ALL_DIRS[@]} directories:"
for dir in "${ALL_DIRS[@]}"; do
    log_info "  - $(basename "$dir")"
done

# ==============================================================================
# Initialize or Resume from Checkpoint
# ==============================================================================

if [ "$RESUME" = true ] && [ -f "$CHECKPOINT_FILE" ]; then
    log_info "Resuming from checkpoint: $CHECKPOINT_FILE"
    
    if [ -f "$COMPLETED_FILE" ]; then
        mapfile -t COMPLETED_DIRS < "$COMPLETED_FILE"
        log_info "Already completed: ${#COMPLETED_DIRS[@]} databases"
    else
        COMPLETED_DIRS=()
    fi
    
    DIRS_TO_PROCESS=()
    for dir in "${ALL_DIRS[@]}"; do
        if [[ ! " ${COMPLETED_DIRS[@]} " =~ " ${dir} " ]]; then
            DIRS_TO_PROCESS+=("$dir")
        fi
    done
    
    log_info "Remaining to process: ${#DIRS_TO_PROCESS[@]} databases"
else
    log_info "Starting fresh processing"
    
    printf "%s\n" "${ALL_DIRS[@]}" > "$CHECKPOINT_FILE"
    > "$COMPLETED_FILE"
    > "$FAILED_FILE"
    
    DIRS_TO_PROCESS=("${ALL_DIRS[@]}")
fi

if [ ${#DIRS_TO_PROCESS[@]} -eq 0 ]; then
    log_success "All databases already processed!"
    exit 0
fi

# ==============================================================================
# Process Each Database
# ==============================================================================

TOTAL_DIRS=${#DIRS_TO_PROCESS[@]}
CURRENT=0
SUCCESS_COUNT=0
FAILED_COUNT=0

log_info "======================================================================"
log_info "Starting processing of $TOTAL_DIRS databases"
log_info "======================================================================"

for dir in "${DIRS_TO_PROCESS[@]}"; do
    CURRENT=$((CURRENT + 1))
    DB_NAME=$(basename "$dir" | sed "s/_${MODE}//")
    
    echo ""
    log_info "----------------------------------------------------------------------"
    log_info "Processing Database ($CURRENT/$TOTAL_DIRS): $DB_NAME"
    log_info "Directory: $dir"
    log_info "----------------------------------------------------------------------"
    
    START_TIME=$(date +%s)
    
    if process_database_with_chunking "$dir"; then
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        
        log_success "Database completed in ${DURATION}s: $DB_NAME"
        echo "$dir" >> "$COMPLETED_FILE"
        SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
    else
        END_TIME=$(date +%s)
        DURATION=$((END_TIME - START_TIME))
        
        log_error "Database failed after ${DURATION}s: $DB_NAME"
        echo "$dir" >> "$FAILED_FILE"
        FAILED_COUNT=$((FAILED_COUNT + 1))
        
        echo ""
        read -p "Continue with remaining databases? (yes/no): " CONTINUE
        if [[ ! "$CONTINUE" =~ ^[Yy]([Ee][Ss])?$ ]]; then
            log_warning "Aborting remaining databases"
            break
        fi
    fi
done

# ==============================================================================
# Final Summary
# ==============================================================================

echo ""
log_info "======================================================================"
log_info "EXECUTION SUMMARY"
log_info "======================================================================"
log_info "Total databases: $TOTAL_DIRS"
log_success "Successfully completed: $SUCCESS_COUNT"
log_error "Failed: $FAILED_COUNT"
log_info "Log file: $LOG_FILE"
log_info "Checkpoint file: $CHECKPOINT_FILE"
log_info "Completed file: $COMPLETED_FILE"

if [ $FAILED_COUNT -gt 0 ]; then
    log_info "Failed file: $FAILED_FILE"
    log_warning "Some databases/tables failed. Review the log and failed file."
    log_info "To resume and retry failed items, run:"
    log_info "  $0 --report-dir \"$REPORT_DIR\" --mode \"$MODE\" --resume"
    log_info ""
    log_info "Table-level checkpoint files are located in: $CHECKPOINT_DIR"
    log_info "  Pattern: tables_<database>_${MODE}/completed_tables.txt"
    log_info "  Pattern: tables_<database>_${MODE}/failed_tables.txt"
fi

log_info "======================================================================"

if [ $FAILED_COUNT -gt 0 ]; then
    exit 1
else
    exit 0
fi
