import logging
import argparse
from cmpsql import CmpSql


log = logging.getLogger()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-t', dest='tag', required=False)
    parser.add_argument('-f1', dest='file1', nargs='*')
    parser.add_argument('-f2', dest='file2', nargs='*')
    parser.add_argument('-a', choices=['load', 'run', 'gen'])
    parser.add_argument('--path', type=str, default='base.db')
    parser.add_argument('--limit', dest='preview_limit', type=int,
                        default=2)
    parser.add_argument('--describe_failed', dest='preview_limit',
                        type=int, default=2)
    parser.add_argument('--append', action='store_true')
    parser.add_argument('--round', type=int,
                        help="allow sqlite function `OR ROUND(X,Y)`")
    parser.add_argument('--no-normalize', action='store_true')
    parser.add_argument('-m', '--manually', action='store_true')
    parser.add_argument('--log-level', default='INFO', type=str)
    parser.add_argument('--keys1', type=str, nargs='*', default=[])
    parser.add_argument('--keys2', type=str, nargs='*', default=[])
    parser.add_argument('--fields1', type=str, nargs='*', default=[])
    parser.add_argument('--fields2', type=str, nargs='*', default=[])
    parser.add_argument('--silent', action='store_true')

    args = parser.parse_args()

    logging.basicConfig(level=args.log_level,
                        format='%(filename)s.%(funcName)s:[%(lineno)d] '
                               '- %(levelname)-8s : %(message)s')
    log.info(f"args: {args}")

    if args.a == 'gen':
        pass
    elif args.manually:
        manual(args)
    else:
        cmp = CmpSql(
            path=args.path,
            tag_name=args.tag,
            file1=args.file1, file2=args.file2,
            keys1=args.keys1, keys2=args.keys2,
            fields1=args.fields1, fields2=args.fields2,
            round_prec=args.round,
            preview_limit=args.preview_limit,
            manually=args.manually,
        )


if __name__ == '__main__':
    main()
