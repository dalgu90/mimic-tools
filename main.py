import argparse
import logging
import os
import time

from mimic_w2v_tools.tools import ensure_dir
from mimic_w2v_tools.extract import extract_mimic_documents
from mimic_w2v_tools.transform import regroup_patient_documents, replace_placeholders, clean_mimic_corpus
from mimic_w2v_tools.ctakes import ctakes_corpus

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    parser.add_argument("-n", "--n_jobs", help="Number of processes", dest="n_jobs", type=int, default=1)

    subparsers = parser.add_subparsers(title="Sub-commands", description="Valid sub-commands",
                                       help="Valid sub-commands", dest="subparser_name")
    # MIMIC document extraction
    parser_extract = subparsers.add_parser('EXTRACT', help="Extract MIMIC documents from database")
    parser_extract.add_argument("--url", help="Database url", dest="url", type=str, required=True)
    parser_extract.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)

    # MIMIC document regroup
    parser_regroup = subparsers.add_parser('REGROUP', help="Create one document per patient")
    parser_regroup.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_regroup.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_regroup.add_argument("-n", "--n_jobs", help="Number of processes", dest="n_jobs", type=int, default=1,
                                required=True)

    # MIMIC palceholders replace
    parser_replace = subparsers.add_parser('REPLACE', help="Create one document per patient")
    parser_replace.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_replace.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_replace.add_argument("--list_dir", help="List directory", dest="list_dir", type=str, required=True)

    # MIMIC clean files
    parser_clean = subparsers.add_parser('CLEAN', help="Clean mimic documents")
    parser_clean.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_clean.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_clean.add_argument("-n", "--n_jobs", help="Number of processes", dest="n_jobs", type=int, default=1,
                              required=True)

    # MIMIC document ctakes processing
    parser_ctakes = subparsers.add_parser('CTAKES', help="Clean mimic documents")
    parser_ctakes.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_ctakes.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_ctakes.add_argument("--ctakes_server", help="Output directory", dest="ctakes_server", type=str,
                               required=True)

    args = parser.parse_args()

    if args.subparser_name == "EXTRACT":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(target_dir)

        log_file_path = os.path.join(target_dir, "extraction-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting document extraction from mimic-iii database")

        extract_mimic_documents(args.url, args.output_dir)

    elif args.subparser_name == "REGROUP":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(os.path.abspath(args.output_dir))

        log_file_path = os.path.join(os.path.abspath(args.output_dir), "extraction-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting document regrouping")

        regroup_patient_documents(args.input_dir, args.output_dir, n_jobs=args.n_jobs)

    elif args.subparser_name == "REPLACE":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(target_dir)

        log_file_path = os.path.join(target_dir, "replace-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting placeholder replacing")

        replace_placeholders(args.input_dir, args.output_dir, args.list_dir)

    elif args.subparser_name == "CLEAN":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(target_dir)

        log_file_path = os.path.join(target_dir, "clean-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting corpus cleaning")

        clean_mimic_corpus(args.input_dir, args.output_dir, n_jobs=args.n_jobs)

    elif args.subparser_name == "CTAKES":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(os.path.abspath(args.output_dir))

        log_file_path = os.path.join(os.path.abspath(target_dir), "ctakes-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Processing files with cTAKES")
        logging.info("============================")
        logging.info("* Remote server location: {}".format(args.ctakes_server))
        logging.info("* Input directory: {}".format(os.path.abspath(args.input_dir)))
        logging.info("* Output directory: {}".format(os.path.abspath(args.output_dir)))

        ctakes_corpus(args.input_dir, args.output_dir, args.ctakes_server)
