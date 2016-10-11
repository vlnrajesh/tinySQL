#!/usr/bin/env python
try:
    import sys
    from select_parser import *
except ImportError, err:
    print err

def read_input(prompt='tinySQL>'):
    """
    :param prompt:
    :return:
    """
    _result = ''
    _delimeter = ';'
    _read_next_line = True
    try:
        while _read_next_line:
            _statement = raw_input(prompt)
            prompt = '   ...> '
            if _statement == '.quit':
                    sys.exit(0)
            if len(_statement) > 0:

                if _delimeter in _statement:
                    _result += ' ' + _statement.partition(_delimeter)[0]
                    _read_next_line = False
                else:
                    _result += ' ' + _statement

            if _statement == '.tables':
                try:
                    _result = ''
                    prompt = 'tinySQL>'
                    _tables = read_tables().keys()
                    print ' '.join(_tables)
                except AttributeError, err:
                    print err
        return _result.strip()
    except KeyboardInterrupt, err:
        return None


def main():
    final_statement=read_input()
    table_db, column_db =parse_n_validate(final_statement)
    fetch_records(table_db, column_db)

if __name__ == '__main__':
    main()
