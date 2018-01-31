import json
import logging
import os
import re

import requests
from joblib import Parallel, delayed

from .tools import remove_abs, ensure_dir

PARAMS = {"annotators": "tokenize,ssplit", "outputFormat": "json"}


def segment_and_tokenize(corpus_path, output_path, corenlp_url, n_jobs=10):
    """
    Segment and tokenize a corpus using CoreNLP
    :param corpus_path: input corpus path (.txt files)
    :param output_path: path where tokenized versions will be stored
    :param corenlp_url: CoreNLP server URL
    :param n_jobs: number of processes to use
    :return: nothing
    """

    processing_list = list()

    logging.info("Gathering file list")

    for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
        for filename in files:

            # Source file path
            source_file = os.path.join(root, filename)

            # Source file subdirectory path
            subdir = remove_abs(re.sub(os.path.abspath(corpus_path), "", root))

            # Target
            target_dir = os.path.join(os.path.abspath(output_path), subdir)
            target_file = os.path.join(target_dir, filename)

            ensure_dir(target_dir)

            processing_list.append((source_file, target_file))

    logging.info("* Number of files: {}".format(len(processing_list)))
    logging.info("Starting processing with {} jobs".format(n_jobs))

    dismissed = Parallel(n_jobs=n_jobs)(delayed(_process_file)(source_file, target_file, corenlp_url)
                                        for source_file, target_file in processing_list)

    logging.info("Dismissed: {:,} chunks, {:,} characters".format(
        sum([item[0] for item in dismissed]),
        sum([item[1] for item in dismissed])
    ))


def _process_file(source_file, target_file, corenlp_url):
    """
    Process one file with CoreNLP. Files are chunked into pieces of roughly 20,000 characters.
    :param source_file: source file path
    :param target_file: target file path
    :param corenlp_url: CoreNLP server URL
    :return: dismissed chunks
    """

    dismissed = [0, 0]

    with open(target_file, "w", encoding="UTF-8") as output_file:
        with open(source_file, "r", encoding="UTF-8") as input_file:

            current_chunk = list()
            current_chunk_char_len = 0

            for line in input_file:

                current_chunk.append(line)
                current_chunk_char_len += len(line)

                if current_chunk_char_len >= 20000:
                    payload = get_response("".join(current_chunk), corenlp_url)
                    current_chunk.clear()
                    current_chunk_char_len = 0

                    if payload:
                        for sentence in payload["sentences"]:
                            current_sentence = list()

                            for token in sentence["tokens"]:
                                current_sentence.append(token["originalText"])

                            output_file.write("{}\n".format(" ".join(current_sentence)))

                    else:
                        dismissed[0] += 1
                        dismissed[1] += current_chunk_char_len

            # Last chunk
            if current_chunk_char_len > 0:
                payload = get_response("".join(current_chunk), corenlp_url)

                if payload:
                    for sentence in payload["sentences"]:
                        current_sentence = list()

                        for token in sentence["tokens"]:
                            current_sentence.append(token["originalText"])

                        output_file.write("{}\n".format(" ".join(current_sentence)))

                else:
                    dismissed[0] += 1
                    dismissed[1] += current_chunk_char_len

    return dismissed


def get_response(txt, corenlp_url):
    """
    Submit text to be tokenized to the CoreNLP server
    :param txt: txt to be tokenized
    :param corenlp_url: CoreNLP server URL
    :return: None or json response
    """

    try:
        # Sending chunk to the server to be processed
        r = requests.post(corenlp_url, params=PARAMS, data=txt.encode("UTF-8"))
    except Exception as e:
        print("Exception while sending request: \"{}\"".format(e))
        return None

    if r.status_code != 200:
        # Wrong code returned, skipping the chunk
        print("Skipping chunk, status code != 200")
        return None

    try:
        payload = r.text
        payload = json.loads(payload, strict=False)
    except Exception as e:
        # Answer is not properly formatted, skipping the chunk
        print("Exception while parsing json: \"{}\"".format(e))
        return None

    return payload
