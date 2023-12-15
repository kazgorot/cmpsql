#!/bin/env python

import csv
import random
import string
from pathlib import Path
import argparse
from typing import Optional, Dict
from tqdm import tqdm


full = string.digits + string.ascii_letters


def _random_string(max_chars=100, min_chars=40):
    return ''.join([random.choice(full)
                    for _ in range(random.randint(min_chars, max_chars))])



other_fields = ['Aaa', 'Bbb', 'Ccc', 'Dddd']


def gen_row(keys):
    return list([
        # f"{x}": f"{random.random()}" for x in keys
        f"{_random_string()}" for x in keys
    ])


def gen_files(p1: str, p2: str,
              keys1=[],
              keys2=[],
              fields1: Optional[list[str]] = [], fields2: Optional[list[str]] = [],
              skip1: Optional[list[int]] = [], skip2: Optional[list[int]] = [],
              dup1: Optional[list[int]] = [], dup2: Optional[list[int]] = [],
              failed: Optional[Dict[int, str]]=dict(),
        nrows=1*10**3,
):
    f1 = Path(p1)
    f2 = Path(p2)

    with open(f1, 'w') as fw1, open(f2, 'w') as fw2:
        cw1 = csv.writer(fw1)
        cw2 = csv.writer(fw2)
        cw1.writerow(fields1)
        cw2.writerow(fields2)

        for i in tqdm(range(nrows), desc='1,2'):
            row1 = gen_row(fields1)
            row2 = list(row1)

            if i in failed:
                assert i not in skip1
                assert i not in skip2
                field1 = fields1[i]
                field2 = fields2[i]
                row1[i] = f"FAIL_A_{field1}_{i}"
                row2[i] = f"FAIL_B_{field2}_{i}"

            # print('row', row1)
            if i not in skip1:
                cw1.writerow(row1)
            # print('row2', row2)
            if i not in skip2:
                cw2.writerow(row2)

            if i in dup1:
                cw1.writerow(row1)
            if i in dup2:
                cw2.writerow(row2)

            # gen_row(keys2)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('filea')
    parser.add_argument('fileb')
    parser.add_argument('--rows', type=int, default=1000)

    parser.add_argument('--keys1', nargs='*', default=[])
    parser.add_argument('--keys2', nargs='*', default=[])

    parser.add_argument('--fields1', nargs='*', default=[])
    parser.add_argument('--fields2', nargs='*', default=[])

    parser.add_argument('--skip1', nargs='*', default=[])
    parser.add_argument('--skip2', nargs='*', default=[])

    parser.add_argument('--dup1', nargs='*', default=[])
    parser.add_argument('--dup2', nargs='*', default=[])

    parser.add_argument('--failed_row', type=int, default=[])
    parser.add_argument('--failed_field', type=int, default=None)  # TODO

    args = parser.parse_args()

    failed = {}
    if args.failed_field is not None:
        failed[args.failed_row] = args.failed_field

    gen_files(args.filea, args.fileb,
              keys1=args.keys1,
              keys2=args.keys2,
              fields1=args.fields1,
              fields2=args.fields2,
              skip1=args.skip1, skip2=args.skip2,
              dup1=args.dup1, dup2=args.dup2,
              failed=failed,
              nrows=args.rows)
