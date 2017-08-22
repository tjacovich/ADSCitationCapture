#!/usr/bin/env python
import os
import sys
import tempfile
import argparse
import json
from adscc import tasks
from adsputils import setup_logging

logger = setup_logging('run.py')


def run(refids, **kwargs):
    """
    Process file specified by the user.

    :param refids: path to the file containing the citations
    :param kwargs: extra keyword arguments
    :return: no return
    """

    logger.info('Loading records from: {0}'.format(refids))

    if 'force' in kwargs:
        force = kwargs['force']
    else:
        force = False

    if 'diagnose' in kwargs:
        diagnose = kwargs['diagnose']
    else:
        diagnose = False

    message = None
    if diagnose:
        print("Calling 'task_check_citation' with '{}'".format(str(message)))
    logger.debug("Calling 'task_check_citation' with '%s'", str(message))
    tasks.task_check_citation.delay(message)


def build_diagnostics(bibcodes=None, json=None):
    """
    Builds a temporary file to be used for diagnostics.
    """
    tmp_file = tempfile.NamedTemporaryFile(delete=False)
    print("Preparing diagnostics temporary file '{}'...".format(tmp_file.name))
    for bibcode, raw_file, provider in zip(bibcodes, raw_files, providers):
        tmp_str = '{}\t{}'.format(bibcode, json)
        print("\t{}".format(tmp_str))
        tmp_file.write(tmp_str+"\n")
    tmp_file.close()
    return tmp_file.name

if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Process user input.')

    parser.add_argument('-r',
                        '--refids',
                        dest='refids',
                        action='store',
                        type=str,
                        help='Path to the refids.dat file'
                             ' that contains the citation list.')

    parser.add_argument('-f',
                        '--force',
                        dest='force',
                        action='store_true',
                        help='Force the processing of all the citations')

    parser.add_argument('-d',
                        '--diagnose',
                        dest='diagnose',
                        action='store_true',
                        default=False,
                        help='Process specific diagnostic default data')

    parser.add_argument('-b',
                        '--bibcodes',
                        dest='bibcodes',
                        action='store',
                        default=None,
                        help='Comma delimited list of bibcodes (for diagnostics)')

    parser.add_argument('-j',
                        '--json',
                        dest='json',
                        action='store',
                        default=None,
                        help='Semicolon delimited list of json citation (for diagnostics)')

    parser.set_defaults(refids=False)
    parser.set_defaults(force=False)
    parser.set_defaults(diagnose=False)

    args = parser.parse_args()

    if args.diagnose:
        if args.bibcodes:
            args.bibcodes = [x.strip() for x in args.bibcodes.split(',')]
        else:
            # Defaults
            args.bibcodes = ["1005PhRvC..71c4906H", "1915PA.....23..189P", "2017PASP..129b4005R"]

        if args.json:
            args.json = [x.strip() for x in args.json.split(';')]
        else:
            # Defaults
            args.json = [
                    "{\"cited\":\"1976NuPhB.113..395J\",\"citing\":\"1005PhRvC..71c4906H\",\"doi\":\"10.1016/0550-3213(76)90133-4\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PhRvC/0071/1005PhRvC..71c4906H.ref.xml.result:17\"}",
                    "{\"cited\":\"...................\",\"citing\":\"2017SSEle.128..141M\",\"score\":\"0\",\"source\":\"/proj/ads/references/resolved/SSEle/0128/10.1016_j.sse.2016.10.029.xref.xml.result:10\",\"url\":\"https://github.com/viennats/viennats-dev\"}"
                    "{\"cited\":\"2013ascl.soft03021B\",\"citing\":\"2017PASP..129b4005R\",\"pid\":\"ascl:1303.021\",\"score\":\"1\",\"source\":\"/proj/ads/references/resolved/PASP/0129/iss972.iop.xml.result:114\"}",
                    ]

        args.refids = build_diagnostics(json=args.json, bibcodes=args.bibcodes)

    if not args.refids:
        print 'You need to give the input list'
        parser.print_help()
        sys.exit(0)

    # Send the files to be put on the queue
    run(args.refids,
        force=args.force,
        diagnose=args.diagnose)

    if args.diagnose:
        print("Removing diagnostics temporary file '{}'".format(args.refids))
        os.unlink(args.refids)
