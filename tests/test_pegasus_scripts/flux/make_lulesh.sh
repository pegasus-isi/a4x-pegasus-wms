#!/usr/bin/env bash

function usage {
    echo "Usage: make_lulesh.sh -i <input> ... -o <output> ..."
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

git clone https://github.com/LLNL/LULESH.git /a4x/orchestration/repo/tests/lulesh
cd /a4x/orchestration/repo/tests/lulesh
mkdir build
cd build
cmake -DCMAKE_CXX_COMPILER=/usr/bin/g++ -DWITH_MPI=ON -DWITH_OPENMP=ON -DMPI_HOME=/usr ..
make
