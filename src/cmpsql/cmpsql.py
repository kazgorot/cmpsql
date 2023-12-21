#!/bin/env python

__version__ = "0.2.1"

import datetime
from pathlib import Path
import logging


try:
    from sqlfile import Sq
except ModuleNotFoundError:
    try:
        from scripts.sqlfile import Sq
    except ModuleNotFoundError:
        from sqlfile import Sq


log = logging.getLogger('cmpsql')
log.setLevel(logging.INFO)


TABLE_OR_VIEW = 'TABLE'


class BITS:
    NOTSET = 0
    MATCHED = 1
    DUPLICATED = 2
    FAILED = 3


SEP_COLUMNS = '/'


class COLORS:
    RED = "\x1b[31;20m"
    GREEN = "\x1b[32;20m"
    YELLOW = "\x1b[33;20m"
    BLUE = "\x1b[34;20m"
    MAGENTA = "\x1b[35;20m"
    CYAN = "\x1b[36;20m"
    WHITE = "\x1b[37;20m"
    GRAY = "\x1b[38;20m"
    END_COLOR = "\x1b[0m"


colored = False


def log_info(s):
    if colored:
        s = f"{COLORS.GREEN}{s}{COLORS.END_COLOR}"
    log.info(s)


def log_error(s):
    if colored:
        s = f"{COLORS.RED}{s}{COLORS.END_COLOR}"
    log.error(s)


def _normalize(field):
    return field.replace('_', '').replace('-', '').lower()


def _normalized(fields):
    return dict({field.replace('_', '').lower(): field for field in fields})


def _tab_key_expression(tab, key_fields):
    """TabA, [Field1, Field2] -->
        `Tab.Field1 || '/' || Tab.Field2`"""
    return " || '/' || ".join(map(lambda x: f"{tab}.{x}", key_fields))


def make_query_failed_values(field_a, field_b, tab_a, keys_a, tab_b, keys_b):
    tke_a = _tab_key_expression(tab_a, keys_a)
    tke_b = _tab_key_expression(tab_b, keys_b)
    query = f"""
    SELECT DISTINCT {tke_a} as KEYS,
        {tab_a}.{field_a} || '/' || {tab_b}.{field_b} as 'VALUES'
    FROM {tab_a}
    INNER JOIN {tab_b}
    ON KEYS = {tke_b}
    WHERE KEYS in (
        SELECT {tke_b} as KEY_VALUES_B FROM {tab_b}
        WHERE {tab_a}.{field_a} != {tab_b}.{field_b}
    )

    AND {tab_a}.{field_a} != {tab_b}.{field_b}
    """
    return query


def _x_expression(tab_a, fields_a, tab_b, fields_b, x='AND'):
    return f' {x} '.join([
        f"({tab_a}.{field_a} = {tab_b}.{field_b})"
        for (field_a, field_b) in zip(fields_a, fields_b)])


def q_rows(tab_a, fields_a, tab_b, fields_b, tag_name='tmp', replace=True):
    """
    create table with matched rows by ROWID
    e.g.
    A_index | B_index
    1       | 7139
    2       | 9556

    """
    log.info('> rows')
    xta = _x_expression('A', fields_a, 'B', fields_b)
    rows_tab = f"{tag_name}_rows"
    q = ""
    if replace:
        q += f"""DROP {TABLE_OR_VIEW} IF EXISTS [{rows_tab}];
                CREATE {TABLE_OR_VIEW} [{rows_tab}] AS """
    else:
        q += f"""INSERT INTO [{rows_tab}] (A_index, B_index) """

    q += f""" SELECT 
        A.rowid AS A_index ,
        B.rowid AS B_index
    FROM [{tab_a}] AS A
    INNER JOIN [{tab_b}] AS B
    ON {xta}
    """
    log.info(f"q_rows: `{q}`")
    return q


def q_mx(tab_a, fields_a, tab_b, fields_b, tag_name='tmp', round_prec=None):
    """
    table matrix of matched rows
    """
    log.debug('> matrix')  # TEMPORARY
    q = f"""DROP {TABLE_OR_VIEW} IF EXISTS [{tag_name}_matrix];"""
    q += f"""CREATE {TABLE_OR_VIEW} [{tag_name}_matrix] AS 
    SELECT A.rowid||'/'||B.rowid as 'A_ROW/B_ROW',"""
    cases = list()
    for field_a, field_b in zip(fields_a, fields_b):
        case = f"""
            CASE WHEN NOT (
                A.{field_a} = B.{field_b}
                -- OR ROUND(A.{field_a}) == round(B.{field_b})
                -- or (A.{field_a} like B.{field_b}) 
                -- or (B.{field_b} like '#%' and A.{field_a} = "")
        """
        if round_prec is not None:
            case += 'OR ROUND(A.' + \
                    f'{field_a}, {round_prec}) ' + \
                    f'== round(B.{field_b}, {round_prec})'
        case += f"""
                )
            THEN A.{field_a}||'/'||B.{field_b}
            ELSE NULL 
            END as '{field_a}/{field_b}'
        """
        cases.append(case)
    q += ',\n\t'.join(cases)
    q += f"""
        FROM [{tab_a}] as A
        INNER JOIN [{tag_name}_rows] as C
        INNER JOIN [{tab_b}] as B
        ON (A.rowid = C.A_index and C.B_index = B.rowid);
    """
    # log.info(f"q: `{q}`")
    return q


def q_fout(fields_a, fields_b, tag_name='tmp'):
    ql = list()
    log.debug('> cmp')
    paired_fields = [
        f'"{field_a}/{field_b}"'
        for (field_a, field_b) in zip(fields_a, fields_b)
    ]
    # or VIEW ?
    q = (f'DROP {TABLE_OR_VIEW} IF EXISTS [{tag_name}_cnt];'
         f'CREATE {TABLE_OR_VIEW} [{tag_name}_cnt] as SELECT ')
    q += ', '.join([f'COUNT({f_}) as {f_}' for f_ in paired_fields])
    q += f' FROM [{tag_name}_matrix]'
    ql.append(q)

    # q = f"""CREATE TABLE [{tag_name}_cnt] as SELECT """ + \
    #     ','.join([f"COUNT({f_})" for f_ in paired_fields]) + \
    #     f"FROM [{tag_name}_matrix];"

    q = (f'DROP {TABLE_OR_VIEW} IF EXISTS [{tag_name}_ftout];'
         f'CREATE {TABLE_OR_VIEW} [{tag_name}_ftout] AS SELECT "A_ROW/B_ROW", ')
    q += ', '.join(paired_fields)
    q += f'FROM [{tag_name}_matrix] WHERE ('
    q += ' or '.join([
        f'"{field_a}/{field_b}" IS NOT NULL '
        for (field_a, field_b) in zip(fields_a, fields_b)
        ])
    q += ')'

    ql.append(q)
    # 1. total counts for each side
    # 2. matched rows
    # 3. failed items

    q = ';'.join(ql) + ';'
    log.debug(f"q: `{q}`")
    return q


def q_dup(table, keys_):
    """TODO: test"""
    keys_sep = ', '.join(keys_)
    _q = f'''ROWID IN (
    SELECT rowid from [{table}] 
    GROUP BY {keys_sep} HAVING COUNT(*) > 1
    )'''
    return _q


def ta(tag): return f"{tag}_A"
def tb(tag): return f"{tag}_B"


class CmpSql:
    def __init__(self, tag_name=None,
                 files_1=None, files_2=None, keys1=[], keys2=[],
                 fields1=[],
                 fields2=[],
                 path=':memory:',
                 manually=False,
                 append=False,
                 round_prec=None, preview_limit=0, silent=True
                 ):

        self.preview_limit = preview_limit
        self.passed = None
        self.round_prec = round_prec

        self.sq = Sq(path, append=append, silent=silent)

        if not manually:

            self.auto(tag_name, files_a=files_1, files_b=files_2,
                      keys_a=keys1, keys_b=keys2,
                      known_a=fields1, known_b=fields2)

    def auto(self, tag_name, files_a, files_b, keys_a, keys_b, known_a, known_b):
        # load and test
        # 1
        if isinstance(files_a, list):
            append = False
            for file_a in files_a:
                self.read_csv('A', tag_name, file_a, append=append)
                append = True
        else:
            self.read_csv('A', tag_name, files_a)

        # 2
        if isinstance(files_a, list):
            append = False
            for file_b in files_b:
                self.read_csv('B', tag_name, file_b, append=append)
                append = True
        else:
            self.read_csv('B', tag_name, files_b)

        self.start(tag_name, keys_a, keys_b, known_a, known_b)

    def read_csv(self, side, tag, file, **kwargs):
        if file is None:
            return None

        if not Path(file).exists():
            log.error(f"no such file: {file}")
            exit(1)

        assert side in ['A', 'B'], side
        tab = f"{tag}_{side}"
        self.sq.read_csv(tab, file, **kwargs)

    def start(self, tag, keys_a, keys_b, fields1, fields2, round_prec=None):
        _start_time = datetime.datetime.now()
        # overwrite default params
        if round_prec is None:
            round_prec = self.round_prec

        # vars
        tab_a = ta(tag)
        tab_b = tb(tag)


        # TODO
        # log.debug(f"dup: {q_dup(tab_a, keys)}")
        # self.sq.mark_with_bit(tab_a, BITS.DUPLICATED, where=q_dup(tab_a, keys_a))
        # self.sq.mark_with_bit(tab_b, BITS.DUPLICATED, where=q_dup(tab_b, keys_b))

        # fields preparations
        fields_a = self.sq.table_columns(tab_a)
        fields_b = self.sq.table_columns(tab_b)

        log.info(f"fields number: {len(fields_a)} vs {len(fields_b)}")

        # function to pair relevant fields
        (m_fields_a, m_fields_b), (
            non_matched_a, non_matched_b) = self.match_fields(
            fields_a, fields_b,
            known_a=fields1, known_b=fields2
        )

        if len(non_matched_a) > 0:
            log.warning(f"non-matched {tab_a} fields: {non_matched_a}")

        if len(non_matched_b) > 0:
            log.warning(f"non-matched {tab_b} fields: {non_matched_b}")
        # <<< FIELD

        log.info(f"total counts: {self.sq.counts()}")

        log.info(f"{q_dup(tab_a, keys_a)}")

        # TODO
        # log.debug(f"dup: {q_dup(tab_a, keys)}")
        # self.sq.mark_with_bit(tab_a, BITS.DUPLICATED, where=q_dup(tab_a, keys_a))
        # self.sq.mark_with_bit(tab_b, BITS.DUPLICATED, where=q_dup(tab_b, keys_b))

        # pair ROWIDs
        q = q_rows(tab_a=tab_a, fields_a=keys_a,
                   tab_b=tab_b, fields_b=keys_b, tag_name=tag)
        self.sq.executescript(q)

        # matrix of all rows
        q = q_mx(tab_a=tab_a, fields_a=m_fields_a,
                 tab_b=tab_b, fields_b=m_fields_b,
                 tag_name=tag, round_prec=round_prec)
        self.sq.executescript(q)

        # VIEWs for non-matched rows
        q = f'''
        CREATE {TABLE_OR_VIEW} IF NOT EXISTS A_out
        AS
        SELECT * FROM [{tab_a}] 
            WHERE rowid NOT IN (
                SELECT A_index FROM [{tag}_rows]
            );
        CREATE {TABLE_OR_VIEW} IF NOT EXISTS B_out
        AS
        SELECT * FROM [{tab_b}] 
            WHERE rowid NOT IN (
                SELECT B_index FROM [{tag}_rows]
            );'''

        self.sq.executescript(q)

        # matrix of failed rows
        q = q_fout(fields_a=m_fields_a, fields_b=m_fields_b,
                   tag_name=tag)
        self.sq.executescript(q)

        self.get_results(tag_name=tag)
        # self.mark_matched(tag_name=tag)
        elapsed = datetime.datetime.now() - _start_time
        log.info(f'> done. Elapsed {elapsed}')

    def mark_matched(self, tag_name):

        tab_a = f'{tag_name}_A'
        tab_b = f'{tag_name}_B'

        q_a = f"ROWID in (select A_index from [{tag_name}_rows])"
        q_b = f"ROWID in (select B_index from [{tag_name}_rows])"
        self.sq.mark_with_bit(tab_a,
                              bit=BITS.MATCHED,
                              where=q_a
                              )
        self.sq.mark_with_bit(tab_b,
                              bit=BITS.MATCHED,
                              where=q_b
                              )

    def drop_(self, tag_name):
        tab_a = f'{tag_name}_A'
        tab_b = f'{tag_name}_B'
        self.sq.execute(f"DELETE * FROM [{tab_a}] "
                        f"WHERE DB_ROW_STATUS = {BITS.MATCHED}")
        self.sq.execute(f"DELETE * FROM [{tab_b}] "
                        f"WHERE DB_ROW_STATUS = {BITS.MATCHED}")

    def get_results(self, tag_name, limit_errs=None):
        # err counts
        res = set()
        counts = {field: err_cnt for (field, err_cnt) in
                  list(self.sq.iter_table(f"{tag_name}_cnt"))[0].items() if
                  err_cnt > 0}

        # duplicated_rows_A = sum((1 for _ in self.sq.iter_by_bit(f'{tag_name}_A', BITS.DUPLICATED)))
        # duplicated_rows_B = sum((1 for _ in self.sq.iter_by_bit(f'{tag_name}_B', BITS.DUPLICATED)))
        # if duplicated_rows_A > 0: res.add(f"duplicated_rows_A: {duplicated_rows_A}")
        # if duplicated_rows_B > 0: res.add(f"duplicated_rows_B: {duplicated_rows_B}")

        if counts:
            log.error(f"failed columns: {counts}")
            # err values
            for i, row in enumerate(
                    self.sq.iter_table(f"{tag_name}_ftout",
                                       req_columns=counts.keys()), start=1):
                for field, val in row.items():
                    if val is None:
                        continue
                    r = tuple([field, val])
                    if r not in res:
                        log.error(f"* {r}")
                    res.add(r)
                    if limit_errs:
                        if len(res) >= limit_errs:
                            break
                if i > 50:  # self.preview_limit and i > self.preview_limit:
                    log.error('...')
                    break

        else:
            log.info('PASSED!')
            self.passed = True
        return counts, res

    def check_counts(self, tag):
        taba, tabb, tab_matched = f"{tag}_A", f"{tag}_B", f"{tag}_rows"
        a = self.sq.counts(taba)
        b = self.sq.counts(tabb)
        c = self.sq.counts(tab_matched)
        passed = a == b == c
        if not passed:
            log.warning(f"matched: {tab_matched}[{c}]; "
                        f"{taba}[{a}]; "
                        f" {tabb}[{b}]"
                        )
        return passed

    @staticmethod
    def match_fields(fields_a, fields_b, known_a=[], known_b=[]):
        matched_a, matched_b = list(), list()

        fields_a = filter(lambda x: x not in ['DB_ROW_STATUS'], fields_a)
        fields_b = filter(lambda x: x not in ['DB_ROW_STATUS'], fields_b)
        # specific fields
        assert len(known_a) == len(known_b)
        for ca, cb in zip(known_a, known_b):
            a_ = fields_a[fields_a.index(ca)]
            b_ = fields_b[fields_b.index(cb)]
            matched_a.append(a_)
            matched_b.append(b_)

        # default mapping
        df1 = _normalized(fields_a)
        df2 = _normalized(fields_b)

        matched_norm = set(df1.keys()) & set(df2.keys())
        non_matched_a_norm = set(df1.keys()) - set(df2.keys())
        non_matched_b_norm = set(df2.keys()) - set(df1.keys())

        for nfield in matched_norm:
            matched_a.append(df1[nfield])
            matched_b.append(df2[nfield])

        non_matched_a = list()
        for nfield in non_matched_a_norm:
            non_matched_a.append(df1[nfield])

        non_matched_b = list()
        for nfield in non_matched_b_norm:
            non_matched_b.append(df2[nfield])

        log.debug(f'* matched: {matched_a} : {matched_b}')
        for a, b in zip(matched_a, matched_b):
            log.debug(f"+ {a} = {b}")
        for na in non_matched_a:
            log.debug(f'-a: {na}')

        for nb in non_matched_b:
            log.debug(f'-b: {nb}')

        return (matched_a, matched_b), (non_matched_a, non_matched_b)

    def replace_values(self):
        pass  # TODO

    def describe_failed(self, tag, i=0, preview_limit=None):
        tab_a = ta(tag)
        tab_b = tb(tag)
        if preview_limit is None:
            preview_limit = self.preview_limit

        for i, row in enumerate(self.sq.iter_table(f"{tag}_ftout"), start=1):

            rowid_a, rowid_b = row.pop(f'A_ROW{SEP_COLUMNS}B_ROW').split(SEP_COLUMNS)
            log.info(f"--------- {rowid_a} / {rowid_b} ---------")
            ra = list(self.sq.iter_table(tab_a, where_clause=f"ROWID = {rowid_a}"))
            rb = list(self.sq.iter_table(tab_b, where_clause=f"ROWID = {rowid_b}"))
            assert len(ra) == 1
            assert len(rb) == 1
            ra = ra[0]
            rb = rb[0]

            max_c = max(map(len, row.keys()))
            for c2 in row.keys():
                f1, f2 = c2.split(SEP_COLUMNS)
                v1, v2 = ra.pop(f1), rb.pop(f2)
                is_failed = row[c2] is not None

                msg = f"{c2:>{max_c}s}: {v1} == {v2}"
                if is_failed:
                    log_error(msg)
                else:
                    log_info(msg)

            log_error("! non matched fields:")
            log_error(f"{ra}")
            log_error(f"{rb}")
            #
            # raise NotImplementedError("describe_failed")
            if i >= preview_limit:
                break  # exit(1)


def manual(args):
    log.info(f"manual: {args}")
    if args.a == 'load':
        append = args.append
        cmp = CmpSql(
            path=args.path,
            tag_name=args.tag,
            file1=args.file1, file2=args.file2,
            keys1=args.keys1, keys2=args.keys2,
            fields1=args.fields1, fields2=args.fields2,
            round_prec=args.round,
            preview_limit=args.preview_limit,
            manually=args.manually,
            append=append,
            silent=args.silent,
        )
        # cmp.read_csv(tag=args.tag, )
        if args.file1:
            for _f in args.file1:
                cmp.read_csv('A', args.tag, file=_f, append=append)
        if args.file2:
            for _f in args.file2:
                cmp.read_csv('B', args.tag, file=_f, append=append)

    elif args.a == 'run':
        cmp = CmpSql(
            path=args.path,
            tag_name=args.tag,
            file1=args.file1, file2=args.file2,
            keys1=args.keys1, keys2=args.keys2,
            fields1=args.fields1, fields2=args.fields2,
            round_prec=args.round,
            preview_limit=args.preview_limit,
            manually=args.manually,
            append=True,
        )
        cmp.start(args.tag, args.keys1, args.keys2,
                  fields1=args.fields1,
                  fields2=args.fields2)
        print(cmp.get_results(args.tag))


if __name__ == '__main__':
    pass
