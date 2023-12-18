#!/usr/bin/env bash

set -xe
rm -f base.db*

../tests/gen_data.py A0.csv B0.csv \
  --fields1 aA bB cC dD eE fF \
  --fields2 AA BB CC DD EE FF \
  --keys1 bB cC \
  --keys2 BB CC \
  --rows 1000000 \
  --failed_row=3 \
  --failed_field=0

../tests/gen_data.py A1.csv B1.csv \
  --fields1 aA bB cC dD eE fF \
  --fields2 AA BB CC DD EE FF \
  --keys1 bB cC \
  --keys2 BB CC \
  --rows 1000000



#python -m cmpsql --log-level WARNING -m -f1 A*.csv -f2 B*.csv -t x -a load
#
python -m cmpsql --log-level INFO -m -f1 A0.csv -t x -a load
python -m cmpsql --log-level INFO -m -f2 B0.csv -t x -a load --append

time python -m cmpsql -t x -m -a run --keys1 aA cC --keys2 AA CC


#python -m cmpsql --log-level INFO -m -f1 A1.csv -t x -a load --append

#python -m cmpsql --log-level INFO -m -f2 B1.csv -t x -a load --append
#python -m cmpsql --log-level INFO -m -f1 A2.csv -t x -a load --append
#python -m cmpsql --log-level INFO -m -f2 B2.csv -t x -a load --append



#python -m cmpsql -t x -m -a run --keys1 a --keys2 A

#python -m cmpsql --log-level INFO -m -f1 A1.csv -t x -a load --append
#python -m cmpsql --log-level INFO -m -f2 B1.csv -t x -a load --append

#python -m cmpsql -t x -m -a run --keys1 a --keys2 A
