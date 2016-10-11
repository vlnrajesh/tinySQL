Pre-requisties:
wget --no-check-certificate https://bootstrap.pypa.io/ez_setup.py
sudo python ez_setup.py
sudo easy_install pip
pip install sqlparse
pip install pandas

Howto:

We can either exectue python main.py and have in interactive terminal or we can directly pass command line argument in single quote
main.py provides multiline program similar to sqlite where select_parser.py accepts as a one liner.

Logic:
1. Accept SQL statement from user
2. Use sqlparse to format the given string(not validating only formatting)
3. Identify table names and columns names and store in respective data structures
4. Identify conditions and conditional operators and update above data structures accordingly
5. Validate given table,columns
6. Load respective table files from file system with pandas data analysis tool

Identified the given statement and applied basic validitions such as
    a. Invalid Query: Minimum length mismatch
    b. We support only SELECT statement at this version
    c. Invalid Query: FROM keyword missing
    d. Invalid Query: Table name(s) not provided
    e. Invalid alias for table
    f. no such physical table
    g. no such table
    h. Invalid query
    i. Invalid Condition/Null Condition specified
    j. Only one AND/OR condition allowed in this version
    k. NOT condition(s) not allowed in this version
    l. No such column
        

Known Issues/Future works:
Pandas is not honoring conditions and projections coming from dynamic inputs. I am in touch with developers for a  fix.
Integrate interactive shell with select_parser for sqlite look a like of parser.
Identified scope of refacting due to time constratints to meet dead lines. I will further update code after Mid-1 exams.
