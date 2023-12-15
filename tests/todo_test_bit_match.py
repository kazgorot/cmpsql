import cmpsql.cmpsql
from cmpsql import CmpSql
# from sqlfile import Sq
import tempfile
from pathlib import Path
from cmpsql.tests._gen_data import gen_files
import logging
log = logging.getLogger()

PATH = 'base3.db'
# PATH = ':memory:'


# def test_1():
#
#     keys = ['a']
#     keys2 = ['a']
#
#     c = CmpSql('tag1', path=PATH, file1=p1, file2=p2, keys=keys, keys2=keys2)
#     failed_fields, failures = c.run_test('tag1')
#     assert c.check_counts('tag1')
#     assert len(failed_fields) == 0
#     assert len(failures) == 0


def test_1():
    with tempfile.TemporaryDirectory() as tmpdirname:
        p1 = Path(tmpdirname) / 'A.csv'
        p2 = Path(tmpdirname) / 'B.csv'
        gen_files(p1=p1, p2=p2, nrows=1 * 10 ** 4,
                  keys1=['a', 'b', 'c'],
                  fields1=['a', 'b', 'c'],
                  keys2=['a', 'b', 'c'],
                  fields2=['a', 'b', 'c'],
                  skip1=[4],
                  failed={2: [(2, 'VALUE1'), (2, 'VALUE2')]},
                  dup1=[6, 7],
                  dup2=[6, 7, 8],
                  )

        keys = ['a']
        keys2 = ['a']

        c = CmpSql('tag1', path=PATH, file1=p1, file2=p2, keys=keys, keys2=keys2)

        failed_fields, failures = c.get_results('tag1')
        c.mark_matched('tag1')

        for row in list(c.sq.iter_by_bit('tag1_A', cmpsql.cmpsql.BITS.DUPLICATED)):
            log.info(f"A: {row}")
        for row in list(c.sq.iter_by_bit('tag1_B', cmpsql.cmpsql.BITS.DUPLICATED)):
            log.info(f"B: {row}")

        assert 4 == len(list(c.sq.iter_by_bit('tag1_A', cmpsql.cmpsql.BITS.DUPLICATED))), len(list(c.sq.iter_by_bit('tag1_A', cmpsql.cmpsql.BITS.DUPLICATED)))
        assert 6 == len(list(c.sq.iter_by_bit('tag1_B', cmpsql.cmpsql.BITS.DUPLICATED)))

        if not c.check_counts('tag1'):
            pass
        assert len(failed_fields) == 0
        assert len(failures) == 0


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(filename)s:%(lineno)d %(levelname)-8s %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S'
                        )
    test_1()
