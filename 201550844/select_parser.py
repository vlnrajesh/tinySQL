#!/usr/bin/env python
try:
    import sqlparse
    import pandas
    import re
    import sys
    import os.path
except ImportError, err:
    print err

def read_tables(filename='metadata.txt'):
    """
    This function reads metadata.txt or any file given filename as input
    and identifies tablename and columns for further process.
    we can invoke this menthod from Interactive terminal '.tables'
    """
    try:
        _metadata = {}
        with open(filename, 'r+') as fp:
            list_of_attrs = fp.read().splitlines()
        if '<begin_table>' not in list_of_attrs or '<end_table>' not in list_of_attrs:
            raise ValueError('Invalid Metadata')

        while list_of_attrs:
            _table_name = list_of_attrs[list_of_attrs.index('<begin_table>') + 1]
            _metadata[_table_name] = list_of_attrs[
                                     list_of_attrs.index('<begin_table>') + 2:list_of_attrs.index('<end_table>')]
            del list_of_attrs[:list_of_attrs.index('<end_table>') + 1]
        return _metadata
    except ValueError, err:
        print >> sys.stderr, err
        return None
    except Exception, err:
        print err

def parse_condition(_cond_statement=list):
    """
    This function validates conditions and updates constraints
    this function also resolves lexical issues
    """
    for each_statement in _cond_statement:
        _index = _cond_statement.index(each_statement)
        if re.match('(\S+)([>|<|=])(\S+)',each_statement):
            each_statement=re.sub('([>|<|=])',r' \1 ', each_statement)
        if re.match('(\S+)([>|<|=])([>|<|=])(\S+)', each_statement):
            each_statement = re.sub('([>|<|=])([>|<|=])',r' \1\2 ', each_statement)
        elif not re.match('\B', each_statement):
            next
        else:
            raise ValueError("Invalid Condition Comparision")
        _cond_statement[_index]=each_statement
    return _cond_statement

def parse_n_validate(statement=None):
    """
    This function identifies the given sql statement and parses it further
    and returns two dicts one table_identifiers and column_identifiers
    """
    try:
        table_identifiers={}
        column_identifiers={}
        _statement = sqlparse.format(statement, keyword_case='upper')
        _statement=(_statement.encode('utf-8')).split(' ')
        _metadata=read_tables()
        query_length = len(_statement)
        if query_length < 4:
            raise ValueError('Invalid Query: Minimum length mismatch')
        if _statement[0]!='SELECT':
            raise ValueError('We support only SELECT statement at this version')
        if 'FROM' not in _statement:
            raise ValueError('Invalid Query: FROM keyword missing')
        else:
            _table_end_index=len(_statement)
            _table_start_index=_statement.index('FROM')+1
            for _word in ('WHERE',):
                if _word in _statement:
                    _table_end_index=_statement.index(_word)
                    break
            _table_names=_statement[_table_start_index:_table_end_index]
            _table_names=' '.join(_table_names)
            _table_names=_table_names.split(',')
            for _each_table in _table_names:
                table_metadata={}
                if len(_each_table)==0:
                    raise ValueError('Invalid Query: Table name(s) not provided')
                if 'AS' in _each_table:
                    _table_name=_each_table.split('AS')[1].strip()
                    table_metadata['table_name']=_each_table.split('AS')[0].strip()
                    table_metadata['table_alias']=_table_name
                    if len(_table_name) == 0:
                        raise ValueError("Invalid alias for %s" % (table_metadata['table_name']))
                else:
                    _table_name=_each_table.strip()
                    table_metadata['table_name'] = _table_name
                table_identifiers[_table_name]=table_metadata
            for _each_table in table_identifiers:
                _table_name=table_identifiers[_each_table]['table_name']
                if not os.path.exists("%s.csv" % (_table_name)):
                    raise ValueError('no such physical table %s' %(_table_name))

                if _table_name not in _metadata.keys():
                    raise ValueError('no such table: %s' %(_table_name))
                else:
                    table_identifiers[_each_table]['columns'] =_metadata[_table_name]
                    table_identifiers[_each_table]['display_columns']=\
                        [_table_name+'.'+column for column in _metadata[_table_name]]

            #Display Columns identification
            _column_start_index = 1
            _column_end_index=_table_start_index-1
            _column_names=_statement[_column_start_index:_column_end_index]
            _column_names=' '.join(_column_names)

            _column_names=_column_names.split(',')
            if '*' in _column_names:
                for _table_name in table_identifiers.keys():
                    _column_names.extend(_metadata[table_identifiers[_table_name]['table_name']])
                del _column_names[_column_names.index('*')]
            for _each_column in _column_names:
                column_metadata={}
                if len(_each_column) == 0:
                    raise ValueError("Invalid Query: Column name(s) not provided")
                column_metadata['order']=_column_names.index(_each_column)
                column_metadata['constraint']=None
                column_metadata['function']=None
                if '(' in _each_column and ')' in _each_column:
                    func_name=_each_column[:_each_column.find('(')]
                    column_metadata['function']=func_name
                    column_metadata['column_name']=_each_column[_each_column.find('(')+1:_each_column.find(')')]
                    _each_column=column_metadata['column_name']
                if 'AS' in _each_column:
                    _column_name=_each_column.split('AS')[1].strip()

                    column_metadata['column_name']=_each_column.split('AS')[0].strip()
                    column_metadata['column_alias']=_column_name
                    if len(_column_name)==0:
                        raise ValueError("Invalid alias for %s" %(column_metadata['column_name']))
                else:
                    _column_name=_each_column.strip()
                    column_metadata['column_name']=_column_name
                    column_metadata['table_name']=None
                if '.' in column_metadata['column_name']:
                    column_metadata['table_name']=column_metadata['column_name'].split('.')[0]
                    column_metadata['column_name']=column_metadata['column_name'].split('.')[1]

                column_identifiers[_column_name]=column_metadata
            #Condition Columns identification
            _cond_statement=None
            for _word in ('WHERE',):
                if _word in _statement:
                    _cond_statement=_statement[_statement.index(_word)+1:]
                    if len(_cond_statement) == 0:
                        raise ValueError('Invalid Condition/Null Condition specified')
                    break
            if not _cond_statement is None:
                if _cond_statement.count('AND') > 1 or _cond_statement.count('OR') >1:
                    raise ValueError('Only one AND/OR condition allowed in this version')
                if _cond_statement.count('NOT') >=1:
                    raise ValueError('NOT condition(s) not allowed in this version')
                operators=['>=' , '<=', '!=', '<>', '>', '<', '=', ]
                _cond_statement=parse_condition(_cond_statement)
                for _expression in _cond_statement:
                    _expression=_expression.split(' ')
                    for _each_operator in operators:
                        if _each_operator in _expression:
                            _index=_expression.index(_each_operator)
                            _cond_table_name=None
                            _left_value = _expression[_index - 1].strip()
                            _constraint = _expression[_index:]
                            if '.' in _left_value:
                                _cond_table_name=_left_value.split('.')[0]
                                _cond_column_name=_left_value.split('.')[1]
                            else:
                                _cond_column_name=_left_value
                            column_identifiers[_cond_column_name]={ 'column_name': _cond_column_name,
                                                                    'table_name':_cond_table_name,
                                                                    'constraint':_constraint,
                                                                    'order': 99,
                                                                    'function': None}
        for _each_column in column_identifiers:
            if column_identifiers[_each_column]['table_name'] is None:
                for table_name in table_identifiers.keys():
                     if column_identifiers[_each_column]['column_name'] in _metadata[table_name]:
                         column_identifiers[_each_column]['table_name']=table_name

            if column_identifiers[_each_column]['table_name'] not in _metadata.keys():
                raise ValueError('No such column:  %s' %(column_identifiers[_each_column]['column_name']))
        return table_identifiers,column_identifiers
    except ValueError as err:
        print >> sys.stderr, err
        sys.exit(1)
        return None,None

def fetch_records(table_db=dict,column_db=dict):
    display_columns=[]
    distinct_columns=[]
    max_of_columns=[]
    avg_of_columns=[]
    sum_of_columns=[]
    min_of_columns=[]
    constraint_columns=[]
    _results=''
    for _each_column in column_db:
        _display_column=column_db[_each_column]['table_name']+'.'+column_db[_each_column]['column_name']
        if _display_column not in display_columns: display_columns.insert(column_db[_each_column]['order'],_display_column)
        function=column_db[_each_column]['function']
        constraint=column_db[_each_column]['constraint']
        if constraint:
            _constraint=_display_column+' '+' '.join(constraint)
            if _constraint not in constraint_columns: constraint_columns.append(_constraint)
        if function == 'distinct':
            if _display_column not in distinct_columns: distinct_columns.append(_display_column)
        if function=='max':
            if _display_column not in max_of_columns: max_of_columns.append(_display_column)
        if function=='avg':
            if _display_column not in avg_of_columns: avg_of_columns.append(_display_column)
        if function=='sum':
            if _display_column not in sum_of_columns: sum_of_columns.append(_display_column)
        if function=='min':
            if _display_column not in min_of_columns: min_of_columns.append(_display_column)

    for _each_table in table_db:
        _table_name=table_db[_each_table]['table_name']
        _read_fp=pandas.read_csv(_table_name+'.csv',names=table_db[_each_table]['display_columns'])
        if constraint_columns:
            print 'TODO: There is a bug in pandas for applying constraint, I am working with developer'
        if distinct_columns:
            _read_fp.drop_duplicates(subset=distinct_columns,inplace=True)
        _write_fp=_read_fp[display_columns]
        if max_of_columns:
            for _each_col in max_of_columns:
                _results+= "%s\t%d\n"%(_each_col,_read_fp[_each_col].max(axis=0))
        elif avg_of_columns:
            for _each_col in avg_of_columns:
                _results+= "%s\t%d\n"%(_each_col,_read_fp[_each_col].mean(axis=0))
        elif sum_of_columns:
            for _each_col in sum_of_columns:
                _results += "%s\t%d\n" % (_each_col, _read_fp[_each_col].sum(axis=0))
        elif min_of_columns:
            for _each_col in min_of_columns:
                _results += "%s\t%d\n" % (_each_col, _read_fp[_each_col].min(axis=0))
        else:
            _results=_write_fp.to_string(index=False)
        print _results



def main():
    table_db, column_db = parse_n_validate(sys.argv[1])
    fetch_records(table_db,column_db)

if __name__ == '__main__':
    main()
