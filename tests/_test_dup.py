

from cmpsql import CmpSql
from gen_data import gen_files
import tempfile
from pathlib import Path
import logging
log = logging.getLogger()


PATH = 'base.db'


def test_2_skip_one_msg2():
    with tempfile.TemporaryDirectory() as tmp_dir:
        p1 = Path(tmp_dir) / 'A.csv'
        p2 = Path(tmp_dir) / 'B.csv'
        gen_files(p1=p1, p2=p2, nrows=10,
                  keys1=['a', 'b', 'c'],
                  fields1=['a', 'b', 'c'],
                  keys2=['a', 'b', 'c'],
                  fields2=['a', 'b', 'c'],
                  dup1 = [1]
                  # skip1=[4],
                  # failed={2: 2}
                  )

        keys = ['a']
        keys2 = ['a']

        c = CmpSql('tag1', path=PATH, file1=p1, file2=p2, keys1=keys, keys2=keys2)
        failed_fields, failures = c.get_results('tag1')
        assert not c.check_counts('tag1')

        assert failures == {'duplicated_rows_A: 1'}, failures
        assert len(failed_fields) == 0, failed_fields
