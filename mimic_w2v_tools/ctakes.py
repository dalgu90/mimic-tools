import gzip
import json
import logging
import os
import re
import subprocess
from collections import defaultdict

import progressbar
import requests
from joblib import Parallel, delayed

from .tools import remove_abs, ensure_dir, get_other_extension


def ctakes_corpus(corpus_path, working_dir, ctakes_server_location):
    """
    Process a clean mimic-iii corpus with ctakes (via remote server)
    :param corpus_path: path to the clean mimic-iii-corpus
    :param working_dir: path where gzipped json files will be created
    :param ctakes_server_location: cTAKES server network address
    :return: nothing
    """

    # Target document path creation
    document_output_path = os.path.join(os.path.abspath(working_dir), "documents")
    ensure_dir(document_output_path)

    logging.info("Counting documents")

    nb_files = 0

    for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
        for _ in files:
            nb_files += 1

    pbar = progressbar.ProgressBar(
        max_value=nb_files,
        widgets=[
            progressbar.Percentage(),
            ' (', progressbar.SimpleProgress(), ') ',
            progressbar.Bar(),
            ' ', progressbar.Timer(), ' ',
            progressbar.AdaptiveETA()
        ]
    )

    logging.info("Start processing")

    # Going trough the files
    track_nb = 0
    skipped = 0

    with pbar as bar:
        for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
            for filename in files:

                # Creating target subdirectory according to source subdirectory
                subdir = remove_abs(re.sub(os.path.abspath(corpus_path), "", root))
                target_dir = os.path.join(os.path.abspath(document_output_path), subdir)

                ensure_dir(target_dir)

                # Source and target file path creation
                source_file = os.path.join(root, filename)
                target_file = os.path.join(os.path.abspath(target_dir), get_other_extension(filename, "json"))

                # Reading source file content
                txt_content = open(source_file, "r", encoding="UTF-8").read()

                if len(txt_content) == 0:

                    track_nb += 1
                    skipped += 1
                    bar.update(track_nb)
                    continue

                # Sending file to cTAKES server
                r = requests.post(ctakes_server_location, data=txt_content.encode("UTF-8"), timeout=None)

                logging.debug("* Status: {}".format(r.status_code))

                # Reading response json content
                r_content = r.json()

                # Dumping json response to disk (target file location)
                json.dump(r_content, open(target_file, "w", encoding="UTF-8"))

                # Gzipping file
                subprocess.call(['gzip', '--best', os.path.basename(target_file)], cwd=target_dir)

                track_nb += 1
                bar.update(track_nb)
                
                logging.info("* Processed {} | Skipped {} | Total {}".format(track_nb, skipped, nb_files))


def ctakes_to_txt(json_path, txt_path, output_path, n_jobs=1):

    document_output_path = os.path.join(os.path.abspath(output_path), "documents")
    ensure_dir(document_output_path)

    processing_list = list()
    # Collecting filenames
    for root, dirs, files in os.walk(os.path.abspath(json_path)):
        for filename in files:
            if re.match("^.*\.json.gz$", filename):
                subdir = remove_abs(re.sub(os.path.abspath(json_path), "", root))
                processing_list.append((root, filename, subdir))

    logging.info("* Number of files to process: {}".format(len(processing_list)))

    logging.info("Starting preparing documents with {} processes".format(n_jobs))
    # Cleaning files
    Parallel(n_jobs=n_jobs)(delayed(_convert_ctakes_file)(root, filename, subdir, os.path.abspath(document_output_path),
                                                          txt_path)
                            for root, filename, subdir in processing_list)

    logging.info("Done !")


def _convert_ctakes_file(root, filename, subdir, document_output_path, txt_path):

    target_path = os.path.join(document_output_path, subdir)
    ensure_dir(target_path)

    source_json_filename = os.path.join(root, filename)

    source_txt_filename = os.path.join(txt_path, subdir, "{}.txt".format(filename.split(".")[0]))
    target_txt_filename = os.path.join(target_path, "{}.txt".format(filename.split(".")[0]))

    json_content = json.load(gzip.open(source_json_filename, 'rt'))
    txt_content = open(source_txt_filename, "r", encoding="UTF-8").read()
    sentences = _fetch_sentences(json_content)

    with open(target_txt_filename, "w", encoding="UTF-8") as output_file:
        for sentence in sorted(sentences):
            sentence_str = " ".join([txt_content[b:e] for b, e in sorted(sentences[sentence])])
            output_file.write("{}\n".format(sentence_str))


def _fetch_sentences(json_content):

    sentences = defaultdict(list)

    sentence_type = "org.apache.ctakes.typesystem.type.textspan.Sentence"

    entity_types = [
        'org.apache.ctakes.typesystem.type.syntax.ContractionToken',
        'org.apache.ctakes.typesystem.type.syntax.NewlineToken',
        'org.apache.ctakes.typesystem.type.syntax.NumToken',
        'org.apache.ctakes.typesystem.type.syntax.PunctuationToken',
        'org.apache.ctakes.typesystem.type.syntax.SymbolToken',
        'org.apache.ctakes.typesystem.type.syntax.WordToken'
    ]

    for ann in json_content:
        if ann["typ"] == sentence_type:
            sentences[(ann["annotation"]["begin"],ann["annotation"]["end"])] = list()

    for ann in json_content:
        if ann["typ"] in entity_types:
            t_begin = ann["annotation"]["begin"]
            t_end = ann["annotation"]["end"]
            for (b, e), tokens in sentences.items():
                if b <= t_begin < t_end <= e:
                    tokens.append((t_begin, t_end))

    return sentences
