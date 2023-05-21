#!/bin/bash
set -e

rm -rf ./demo-tests/case*-dir

cargo test case1
cargo test case2
cargo test case3