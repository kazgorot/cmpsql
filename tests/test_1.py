from cmpsql import CmpSql
from tests.gen_data import gen_files
import tempfile
import random
import string
from pathlib import Path
import logging

log = logging.getLogger()


# PATH = 'base2.db'
PATH = ':memory:'


def _cls(x): return x.lower().replace('_', '').replace('-', '')


def _random_string(max_chars=90, min_chars=10):
    return ''.join([random.choice(string.digits + string.ascii_letters)
                    for _ in range(random.randint(min_chars, max_chars))])


def test_1():
    with tempfile.TemporaryDirectory() as tmp_dir:
        p1 = Path(tmp_dir) / 'A.csv'
        p2 = Path(tmp_dir) / 'B.csv'
        gen_files(p1=p1, p2=p2, nrows=1 * 10 ** 4,
                  keys1=['a', 'b', 'c'],
                  fields1=['a', 'b', 'c'],
                  keys2=['a', 'b', 'c'],
                  fields2=['a', 'b', 'c'],
                  # skip1=[4],
                  # failed={2: 2}
                  )

        keys = ['a']
        keys2 = ['a']

        c = CmpSql('tag1', path=PATH, 
                   file1=p1, file2=p2, 
                   keys1=keys, keys2=keys2)
        failed_fields, failures = c.get_results('tag1')
        assert c.check_counts('tag1')
        assert len(failed_fields) == 0
        assert len(failures) == 0


def test_2_skip_one_msg():
    with tempfile.TemporaryDirectory() as tmp_dir:
        p1 = Path(tmp_dir) / 'A.csv'
        p2 = Path(tmp_dir) / 'B.csv'
        gen_files(p1=p1, p2=p2, nrows=1 * 10 ** 4,
                  keys1=['a', 'b', 'c'],
                  fields1=['a', 'b', 'c'],
                  keys2=['a', 'b', 'c'],
                  fields2=['a', 'b', 'c'],
                  skip1=[4],
                  # failed={2: 2}
                  )

        keys = ['a']
        keys2 = ['a']

        c = CmpSql('tag1', path=PATH, file1=p1, file2=p2, 
                   keys1=keys, keys2=keys2)
        failed_fields, failures = c.get_results('tag1')
        assert not c.check_counts('tag1')
        assert len(failed_fields) == 0, failed_fields
        assert len(failures) == 0, failures


def test_fail_one_msg():
    with tempfile.TemporaryDirectory() as tmp_dir:
        p1 = Path(tmp_dir) / 'A.csv'
        p2 = Path(tmp_dir) / 'B.csv'
        gen_files(p1=p1, p2=p2, nrows=1 * 10 ** 4,
                  keys1=['a', 'b', 'c'],
                  fields1=['a', 'b', 'c'],
                  keys2=['a', 'b', 'C'],
                  fields2=['a', 'b', 'C'],
                  # skip1=[4],
                  failed={2: [(2, 'VALUE1'), (2, 'VALUE2')]}
                  )

        keys = ['a']
        keys2 = ['a']

        c = CmpSql('tag1', path=PATH, file1=p1, file2=p2,
                   keys1=keys, keys2=keys2)

        failed_fields, failures = c.get_results('tag1')
        assert c.check_counts('tag1')
        assert failed_fields == {'c/C': 1}, failed_fields
        assert len(failures) == 1, failures
        assert failures == {('c/C', 'FAIL_A_c_2/FAIL_B_C_2')}, failures


def test_matcher_1():
    keys1 = ['b', 'a']
    keys2 = ['A', 'B']
    ((matched_a, matched_b),
     (non_matched_a, non_matched_b)) = CmpSql.match_fields(
        keys1, keys2)

    # original fields not changed
    assert set(keys1) == set(matched_a), f"{set(keys1), set(matched_a)}"
    assert set(keys2) == set(matched_b), f"{set(keys2), set(matched_b)}"

    # no extra fields
    assert len(non_matched_a) == 0
    assert len(non_matched_b) == 0

    # correct pairs
    for a, b in zip(matched_a, matched_b):
        assert _cls(a) == _cls(b), f"{[a, b]}"
        print(a, b)

    # assert False, [(matched_a, matched_b), (non_matched_a, non_matched_b)]


def test_matcher_extra_fields():
    fields1 = ['bob', 'alic_e', 'cow', 'Elf']
    fields2 = ['Alic_e', 'Bob', 'Dog']
    ((matched_a, matched_b),
     (non_matched_a, non_matched_b)) = CmpSql.match_fields(fields1, fields2)

    # original fields not changed
    assert set(matched_a) == {'alic_e', 'bob'}, \
        f"{set(fields1), set(matched_a)}"
    assert set(matched_b) == {'Alic_e', 'Bob'}, \
        f"{set(fields2), set(matched_b)}"

    # extra fields
    assert len(non_matched_a) == 2
    assert len(non_matched_b) == 1

    assert set(non_matched_a) == {'cow', 'Elf'}, non_matched_a
    assert set(non_matched_b) == {'Dog'}, non_matched_b

    # correct pairs
    for a, b in zip(matched_a, matched_b):
        # a == A
        assert _cls(a) == _cls(b), f"{[a, b]}"

    # assert False, [(matched_a, matched_b), (non_matched_a, non_matched_b)]


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s:'
                               '%(lineno)d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
