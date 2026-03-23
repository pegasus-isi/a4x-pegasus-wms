#!/usr/bin/env bash

function usage {
    echo "Usage: run_lulesh_300_20.sh -i <input> ... -o <output> ..."
    echo "Options:"
    echo "========"
    echo "  * -i <input>: provides an input filename. Can provide multiple times."
    echo "                Order should match the inputs provided to the Task/Job."
    echo "  * -o <output>: provides an output filename. Can provide multiple times."
    echo "                Order should match the outputs provided to the Task/Job."
}

inputs=()
outputs=()

while getopts "i:o:h" opt; do
    case $opt in
        i)
            inputs+=("$OPTARG")
            ;;
        o)
            outputs+=("$OPTARG")
            ;;
        h)
            usage
            exit 0
            ;;
        \?)
            echo "Invalid option: -$OPTARG" >&2
            usage
            exit 1
            ;;
    esac
done

shift $((OPTIND - 1))

flux run --nodes=1 --ntasks=1 --cores-per-task=1 --exclusive /a4x/orchestration/repo/tests/lulesh/build/lulesh2.0 -s 300 -i 20 -p
