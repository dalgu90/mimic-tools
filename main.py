import argparse
import logging
import os
import sys
import time
from datetime import timedelta

from mimic.ctakes import ctakes_corpus, ctakes_to_txt
from mimic.extract import extract_mimic_documents
from mimic.tools import ensure_dir
from mimic.transform import replace_placeholders, clean_mimic_corpus
from mimic.corenlp import segment_and_tokenize
from mimic.w2v import build_model

if __name__ == "__main__":

    parser = argparse.ArgumentParser()

    subparsers = parser.add_subparsers(title="Sub-commands", description="Valid sub-commands",
                                       help="Valid sub-commands", dest="subparser_name")

    # MIMIC document extraction from database
    parser_extract = subparsers.add_parser('EXTRACT', help="Extract MIMIC documents from database")
    parser_extract.add_argument("--url", help="Database URL", dest="url", type=str, required=True)
    parser_extract.add_argument("--output-dir", help="Output directory", dest="output_dir", type=str, required=True)

    # MIMIC placeholders replacement
    parser_replace = subparsers.add_parser('REPLACE', help="Perform pseudonymization of the documents")
    parser_replace.add_argument("--input-dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_replace.add_argument("--output-dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_replace.add_argument("--list-dir", help="List directory", dest="list_dir", type=str, required=True)

    # # MIMIC clean files
    # parser_clean = subparsers.add_parser('CLEAN', help="Clean MIMIC documents")
    # parser_clean.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    # parser_clean.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    # parser_clean.add_argument("-n", "--n_jobs", help="Number of processes", dest="n_jobs", type=int, default=1,
    #                           required=True)

    # # MIMIC document ctakes processing
    # parser_ctakes = subparsers.add_parser('CTAKES', help="Process MIMIC documents with cTAKES")
    # parser_ctakes.add_argument("--input_dir", help="Input directory", dest="input_dir", type=str, required=True)
    # parser_ctakes.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    # parser_ctakes.add_argument("--ctakes_dir", help="cTAKES directory", dest="ctakes_dir", type=str, required=True)
    # parser_ctakes.add_argument("--java_dir", help="Java bin directory", dest="java_dir", type=str, required=True)
    # parser_ctakes.add_argument("--resources_dir", help="Resource directory", dest="resources_dir", type=str,
    #                            required=True)

    # MIMIC document CoreNLP processing
    parser_corenlp = subparsers.add_parser('CORENLP', help="Process MIMIC documents with CoreNLP")
    parser_corenlp.add_argument("--input-dir", help="Input directory", dest="input_dir", type=str, required=True)
    parser_corenlp.add_argument("--output-dir", help="Output directory", dest="output_dir", type=str, required=True)
    parser_corenlp.add_argument("--url", help="corenlp URL", dest="url", type=str, required=True)
    parser_corenlp.add_argument("-n", "--n-jobs", help="Number of processes", dest="n_jobs", type=int, default=10,
                                required=True)

    # # Sentence and token extraction from cTAKES files
    # parser_txt = subparsers.add_parser('CTAKES-TO-TXT', help="Sentence and token extraction from cTAKES files")
    # parser_txt.add_argument("--xml_dir", help="Input json directory", dest="xml_input_dir", type=str, required=True)
    # parser_txt.add_argument("--txt_dir", help="Input txt directory", dest="txt_input_dir", type=str, required=True)
    # parser_txt.add_argument("--output_dir", help="Output directory", dest="output_dir", type=str, required=True)
    # parser_txt.add_argument("-n", "--n_jobs", help="Number of processes", dest="n_jobs", type=int, default=1,
    #                         required=True)

    # BUILD ONE W2V MODEL
    parser_build_w2v = subparsers.add_parser('BUILD-W2V', help="Build one word2vec model with gensim")
    parser_build_w2v.add_argument("--corpus-dir", help="Input corpus directory", dest="corpus_dir", type=str,
                                  required=True)
    parser_build_w2v.add_argument("--output-dir", help="Output directory where a subdirectory containing the mode will"
                                                       " be created", dest="output_dir", type=str, required=True)

    parser_build_w2v.add_argument("--size", help="Vector size (default: 100)", dest="size", type=int, default=100)
    parser_build_w2v.add_argument("--window", help="Window size (default: 5)", dest="window", type=int, default=5)
    parser_build_w2v.add_argument("--min-count", help="Min count (default: 5)", dest="min_count", type=int, default=5)
    parser_build_w2v.add_argument("--iterations", help="Number of iterations (default: 5)", dest="iterations", type=int,
                                  default=5)
    parser_build_w2v.add_argument("--neg-sample", help="Number of negative samples (default: 5)", dest="neg_sample",
                                  type=int, default=5)
    parser_build_w2v.add_argument("--sample", help="High frequency threshold (default: 0.001)", dest="sample",
                                  type=float, default=0.001)
    parser_build_w2v.add_argument("--alpha", help="Initial learning rate (default 0.025)", dest="alpha", type=float,
                                  default=0.025)

    group_type = parser_build_w2v.add_mutually_exclusive_group(required=True)
    group_type.add_argument('--skip-gram', action='store_true', dest="skip_gram")
    group_type.add_argument('--cbow', action='store_true')

    parser_build_w2v.add_argument("-n", "--n-jobs", help="Number of processes (default: 1)", dest="n_jobs", type=int,
                                  default=1, required=True)

    args = parser.parse_args()

    if args.subparser_name == "EXTRACT":

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(target_dir)

        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting document extraction from mimic-iii database")

        extract_mimic_documents(args.url, target_dir)

    elif args.subparser_name == "REPLACE":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(target_dir)

        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting placeholder replacing")

        replace_placeholders(args.input_dir, target_dir, args.list_dir)

    # elif args.subparser_name == "CLEAN":
    #
    #     timestamp = time.strftime("%Y%m%d-%H%M%S")
    #
    #     target_dir = os.path.join(os.path.abspath(args.output_dir))
    #
    #     if os.path.isdir(target_dir):
    #         raise IsADirectoryError("The output path you specified already exists")
    #
    #     ensure_dir(target_dir)
    #
    #     log_file_path = os.path.join(target_dir, "clean-{}.log".format(timestamp))
    #     logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')
    #
    #     logging.info("Starting corpus cleaning")
    #
    #     clean_mimic_corpus(args.input_dir, args.output_dir, n_jobs=args.n_jobs)

    # elif args.subparser_name == "CTAKES":
    #
    #     timestamp = time.strftime("%Y%m%d-%H%M%S")
    #
    #     target_dir = os.path.join(os.path.abspath(args.output_dir))
    #
    #     if os.path.isdir(target_dir):
    #         raise IsADirectoryError("The output path you specified already exists")
    #
    #     ensure_dir(os.path.abspath(args.output_dir))
    #
    #     log_file_path = os.path.join(os.path.abspath(target_dir), "ctakes-{}.log".format(timestamp))
    #     logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')
    #
    #     logging.info("Processing files with cTAKES")
    #     logging.info("============================")
    #     logging.info("* Input directory: {}".format(os.path.abspath(args.input_dir)))
    #     logging.info("* Output directory: {}".format(os.path.abspath(args.output_dir)))
    #
    #     ctakes_corpus(args.input_dir, args.output_dir, args.ctakes_dir, args.java_dir, args.resources_dir)

    elif args.subparser_name == "CORENLP":

        target_dir = os.path.join(os.path.abspath(args.output_dir))

        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        ensure_dir(os.path.abspath(target_dir))

        logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Processing files with CoreNLP")
        logging.info("=============================")
        logging.info("* Input directory: {}".format(os.path.abspath(args.input_dir)))
        logging.info("* Output directory: {}".format(os.path.abspath(args.output_dir)))

        segment_and_tokenize(args.input_dir, target_dir, args.url, n_jobs=args.n_jobs)

    # elif args.subparser_name == "CTAKES-TO-TXT":
    #
    #     timestamp = time.strftime("%Y%m%d-%H%M%S")
    #
    #     target_dir = os.path.join(os.path.abspath(args.output_dir))
    #
    #     if os.path.isdir(target_dir):
    #         raise IsADirectoryError("The output path you specified already exists")
    #
    #     ensure_dir(os.path.abspath(args.output_dir))
    #
    #     log_file_path = os.path.join(os.path.abspath(target_dir), "ctakesTXT-{}.log".format(timestamp))
    #     logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')
    #
    #     logging.info("Starting sentence and token extraction from cTAKES files")
    #
    #     ctakes_to_txt(args.xml_input_dir, args.txt_input_dir, args.output_dir, args.n_jobs)

    elif args.subparser_name == "BUILD-W2V":

        timestamp = time.strftime("%Y%m%d-%H%M%S")

        # Preparing model prefix depending on the word2vec model type
        if args.skip_gram:
            model_type = "sg"
            model_type_num = 1
        else:
            model_type = "cbow"
            model_type_num = 0

        # Computing model prefix
        model_prefix = "{}-s{:04d}-w{:02d}-m{:03d}-ns{:02d}-s{}-a{}-i{:02d}".format(
            model_type,
            args.size,
            args.window,
            args.min_count,
            args.neg_sample,
            args.sample,
            args.alpha,
            args.iterations
        )

        # Building target model directory
        target_dir = os.path.join(os.path.abspath(args.output_dir), model_prefix)

        # Checking if target model directory exists
        if os.path.isdir(target_dir):
            raise IsADirectoryError("The output path you specified already exists")

        # Creating target directory
        ensure_dir(target_dir)

        # Logging to a file withing the target directory
        log_file_path = os.path.join(os.path.abspath(target_dir), "build-w2v-{}.log".format(timestamp))
        logging.basicConfig(filename=log_file_path, level=logging.INFO, format='%(asctime)s %(message)s')

        logging.info("Starting word2vec model computation")
        logging.info("* corpus directory: {}".format(os.path.abspath(args.corpus_dir)))
        logging.info("* output directory: {}".format(os.path.abspath(args.output_dir)))
        logging.info("* vector size: {}".format(args.size))
        logging.info("* window: {}".format(args.window))
        logging.info("* min-count: {}".format(args.min_count))
        logging.info("* nb. of iterations: {}".format(args.iterations))
        logging.info("* negative sample rate: {}".format(args.neg_sample))
        logging.info("* high frequency threshold: {}".format(args.sample))
        logging.info("* initial learning rate: {}".format(args.alpha))
        if args.skip_gram:
            logging.info("* using skip-gram algorithm")
        else:
            logging.info("* using cbow algorithm")
        logging.info("* number of processes: {}".format(args.n_jobs))

        start = time.time()

        # Launching model computation
        build_model(args.corpus_dir, target_dir, model_prefix, size=args.size, window=args.window,
                    min_count=args.min_count, sg=model_type_num, n_jobs=args.n_jobs, iterations=args.iterations,
                    neg_sample=args.neg_sample, sample=args.sample, alpha=args.alpha)

        end = time.time()

        logging.info("Done ! (Time elapsed: {})".format(timedelta(seconds=round(end - start))))
