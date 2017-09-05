import logging
import os
import random
import re
from collections import defaultdict

import gensim
import joblib
from joblib import Parallel, delayed

from .tools import ensure_dir, remove_abs


class SentenceIterator:

    def __init__(self, corpus_path):

        self.corpus_path = corpus_path
        self.document_list = list()

        for root, dirs, files in os.walk(os.path.abspath(self.corpus_path)):
            for filename in files:
                self.document_list.append(os.path.join(root, filename))

    def __iter__(self):

        random.shuffle(self.document_list)

        for filename in self.document_list:
            with open(filename, "r", encoding="UTF-8") as input_file:
                for line in input_file:
                    if re.match("^$", line):
                        continue

                    tokens = line.rstrip("\n").split(" ")

                    yield tokens


def build_model(input_directory, output_directory, size=200, window=4, min_count=8, sg=0, n_jobs=1, iterations=5):

    if sg == 0:
        model_type = "cbow"
    else:
        model_type = "sg"

    # Computing model prefix
    model_prefix = "{}-s{:04d}-w{:02d}-m{:03d}-i{:02d}".format(
        model_type,
        size,
        window,
        min_count,
        iterations
    )

    # # Computing target directory
    # target_dir = os.path.join(output_directory, model_prefix)
    # ensure_dir(target_dir)

    # Computing target file path
    target_model_name = os.path.join(output_directory, '{}.pkl'.format(model_prefix))

    logging.info("Loading files in iterator")
    sentences = SentenceIterator(input_directory)

    logging.info("Building model")
    model = gensim.models.Word2Vec(sentences, workers=n_jobs, iter=iterations,
                                   window=window, size=size, min_count=min_count, sg=sg)

    logging.info("Dumping model to file")
    model.save(target_model_name)


def prep_w2v(input_dir, output_dir, n_jobs=1, use_unknown_token=False, ratio_unknown=0.5, lowercase=True,
             replace_digits=True):
    """
    Prepare mimic corpus for word2vec model learning
    :param use_unknown_token:
    :param input_dir: path to the input documents (one sentence per line, tokens separated with spaces)
    :param output_dir: path where output files will be created
    :param n_jobs: number of processes to use
    :param ratio_unknown: ratio for unk token replacement
    :param lowercase: flag to lowercase tokens
    :param replace_digits: flag to replace token digits by 0
    :return: nothing
    """

    # Path where output document will be created
    document_output_path = os.path.join(os.path.abspath(output_dir), "documents")
    ensure_dir(document_output_path)

    # Path where the singletons file will be dumped
    singletons_file_path = os.path.join(os.path.abspath(output_dir), "singletons.pkl")

    logging.info("Gathering documents...")
    processing_list = list()

    # Collecting filenames
    for root, dirs, files in os.walk(os.path.abspath(input_dir)):
        for filename in files:
            if re.match("^.*\.txt$", filename):
                subdir = remove_abs(re.sub(os.path.abspath(input_dir), "", root))
                processing_list.append((root, filename, subdir))

    if use_unknown_token:
        # Fetching token count (map-reduce)
        all_tokens = defaultdict(int)

        logging.info("Fetching token count")

        results = Parallel(n_jobs=n_jobs)(delayed(_gather_token_count)(root, filename, lowercase=lowercase,
                                                                       replace_digits=replace_digits)
                                          for root, filename, _ in processing_list)

        logging.info("Merging list")

        # Total number of tokens
        token_nb = 0

        # Combining results
        for result in results:
            for k, v in result.items():
                all_tokens[k] += v
                token_nb += v

        # Assembling singleton list
        singletons_all = set()
        singletons_sample = set()

        for k, v in all_tokens.items():
            if v == 1 and not re.search("\d", k):
                singletons_all.add(k)
                if random.random() < ratio_unknown:
                    singletons_sample.add(k)

        logging.info("* Number of singletons: {}".format(len(singletons_all)))
        logging.info("* Number of singletons in the sample ({}): {}".format(ratio_unknown, len(singletons_sample)))
        logging.info("* Total number of tokens: {}".format(token_nb))

        logging.info("Dumping singleton list to disk")
        _ = joblib.dump(singletons_sample, singletons_file_path)

    logging.info("Chunking file list")
    file_path_chunks = _chunk_list(processing_list, n_jobs)

    logging.info("Starting processing files")
    Parallel(n_jobs=n_jobs)(delayed(_write_file)(file_path_list, use_unknown_token, singletons_file_path,
                                                 document_output_path)
                            for file_path_list in file_path_chunks)

    logging.info("Done !")


def _gather_token_count(root, filename, lowercase=True, replace_digits=True):
    """
    Count tokens for a given document
    :param root: absolute path to the document
    :param filename: document file name
    :param lowercase: should we lowercase tokens?
    :param replace_digits: should we replace digits by 0?
    :return: token count dictionary
    """

    # Will contain token counts
    document_tokens = defaultdict(int)

    # File full path
    source_txt_file = os.path.join(root, filename)

    with open(source_txt_file, "r", encoding="UTF-8") as input_file:
        for line in input_file:

            if re.match("^$", line):
                continue

            tokens = line.rstrip("\n").split(" ")

            for token_str in tokens:

                if lowercase:
                    token_str = token_str.lower()

                if replace_digits:
                    token_str = re.sub("\d", "0", token_str)

                document_tokens[token_str] += 1

    return document_tokens


def _write_file(file_path_list, use_unknown_token, singletons_file_path, document_output_path, lowercase=True,
                replace_digits=True):
    """
    Write file to disk
    :param file_path_list: list of files to process
    :param singletons_file_path: path to the singleton list
    :param document_output_path: output top directory
    :param lowercase: flag to lowercase tokens
    :param replace_digits: flag to replace token digits with 0
    :return: nothing
    """

    if use_unknown_token:
        # Loading singleton file
        singletons = joblib.load(os.path.abspath(singletons_file_path))

    for root, filename, subdir in file_path_list:

        # Creating target directory
        ensure_dir(os.path.join(os.path.abspath(document_output_path), subdir))

        # Source and target file paths
        source_file_path = os.path.join(root, filename)
        target_file_path = os.path.join(os.path.abspath(document_output_path), subdir, filename)

        with open(source_file_path, "r", encoding="UTF-8") as input_file:
            with open(target_file_path, "w", encoding="UTF-8") as output_file:
                for line in input_file:

                    # Skipping blank lines
                    if re.match("^$", line):
                        continue

                    sent_tokens = list()

                    for tok in line.rstrip("\n").split(" "):

                        # Lowercasing
                        if lowercase:
                            tok = tok.lower()

                        # Replacing digits
                        if replace_digits:
                            tok = re.sub("\d", "0", tok)

                        if use_unknown_token:
                            # Replacing token if it is in singleton list
                            if tok in singletons:
                                tok = "#unk#"

                        sent_tokens.append(tok)

                    # Writing sentence to file
                    output_file.write("{}\n".format(" ".join(sent_tokens)))


def _chunk_list(the_list, nb_parts):
    """
    Divide a list into chunk of equal size
    :param the_list: list to chunk
    :param nb_parts: number of chunks
    :return: list of lists
    """

    division = len(the_list) / nb_parts
    return [the_list[round(division * i):round(division * (i + 1))] for i in range(nb_parts)]
