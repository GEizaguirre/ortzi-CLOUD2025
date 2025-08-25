#!/bin/bash

# Check if the required arguments are provided
if [ "$#" -gt 0 ] && ([ "$1" == "-h" ] || [ "$1" == "--help" ]); then
    echo "Usage: $0 [test: terasort|tpc-ds|tpc-ds-sort|all] [log_dir] [backend: CLOUD_FUNCTION|CONTAINER|MULTIPROCESSING|K8S]"
    echo "Default test: all"
    echo "Default log_dir: ./logs"
    echo "Default backend: MULTIPROCESSING"
    exit 0
fi

TEST_TYPE=${1:-all}
LOG_DIR=${2:-./logs}
BACKEND=${3:-MULTIPROCESSING}

mkdir -p "$LOG_DIR"

run_terasort_tests() {

    mkdir -p "$LOG_DIR/terasort"

    echo "Terasort"
    echo "====================="

    echo "Proactive with simple executors"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 1 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --validate

    echo "Proactive with custom fanout"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 1 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --fanout1 2 \
        --fanout2 11 \
        --validate

    echo "Proactive with big executors"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 2 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --validate

    echo "Proactive with big executors (scale from 0)"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --validate

    echo "Proactive using memory"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 1 \
        --workers 1 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --fanout1 1 \
        --fanout2 1 \
        --validate

    echo "Proactive using memory (scale from 0)"
    python3 examples/execution_modes/terasort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/terasort/" \
        --validate

}

run_tpc_ds_sort_tests() {
    echo "TPC-DS sort"
    echo "====================="

    mkdir -p "$LOG_DIR/tpcd-ds-sort"

    echo "Proactive with simple executors"
    python3 examples/execution_modes/tpc-ds-sort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 1 \
        --autoscale \
        --log-dir "$LOG_DIR/tpcd-ds-sort"

    echo "Proactive with big executors"
    python3 examples/execution_modes/tpc-ds-sort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 2 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpcd-ds-sort"

    echo "Proactive with big executors (scale from 0)"
    python3 examples/execution_modes/tpc-ds-sort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpcd-ds-sort"

    echo "Proactive using memory"
    python3 examples/execution_modes/tpc-ds-sort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 2 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpcd-ds-sort"

    echo "Proactive using memory (scale from 0)"
    python3 examples/execution_modes/tpc-ds-sort.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpcd-ds-sort"

}

run_tpc_ds_tests() {
    echo "TPC-DS"
    echo "====================="

    mkdir -p "$LOG_DIR/tpc-ds"

    echo "Proactive with simple executors"
    python3 examples/execution_modes/tpc-ds.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 1 \
        --autoscale \
        --log-dir "$LOG_DIR/tpc-ds"

    echo "Proactive with big executors"
    python3 examples/execution_modes/tpc-ds.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 2 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpc-ds"

    echo "Proactive with big executors (scale from 0)"
    python3 examples/execution_modes/tpc-ds.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage DISK \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpc-ds"

    echo "Proactive using memory"
    python3 examples/execution_modes/tpc-ds.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 2 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpc-ds"

    echo "Proactive using memory (scale from 0)"
    python3 examples/execution_modes/tpc-ds.py \
        --provisioner-mode PROACTIVE \
        --backend $BACKEND \
        --storage MEMORY \
        -t \
        --executors 0 \
        --workers 4 \
        --autoscale \
        --log-dir "$LOG_DIR/tpc-ds"

}

case $TEST_TYPE in
    terasort)
        run_terasort_tests
        ;;
    tpc-ds-sort)
        run_tpc_ds_sort_tests
        ;;
    tpc-ds)
        run_tpc_ds_tests
        ;;
    all)
        run_terasort_tests
        run_tpc_ds_sort_tests
        run_tpc_ds_tests
        ;;
    *)
        echo "Invalid test type: $TEST_TYPE"
        echo "Valid options are: terasort, tpc-ds, tpc-ds-sort, all"
        exit 1
        ;;
esac