# This module is executed when the program is run as a module...
# using ``python -m solrzkutil``
import sys

if __name__ == '__main__':
    try:
        import solrzkutil
    except ImportError as e:
        sys.exit('solrzkutil python package is not installed. %s\n' % e)

    from solrzkutil import main

    try:
        sys.exit(main(sys.argv))
    except KeyboardInterrupt:
        sys.exit('\n')