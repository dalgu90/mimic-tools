import json
import logging
import os
import re
import subprocess
import progressbar

import requests

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

    # New 'requests' session
    s = requests.Session()

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

    # Going trough the files
    track_nb = 0

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

                logging.info("Processing {}/{} ({} chars)".format(subdir, filename, len(txt_content)))

                # Sending file to cTAKES server
                r = s.post(ctakes_server_location, data=txt_content.encode("UTF-8"), timeout=None)

                logging.debug("* Status: {}".format(r.status_code))

                # Reading response json content
                r_content = r.json()

                # Dumping json response to disk (target file location)
                json.dump(r_content, open(target_file, "w", encoding="UTF-8"))

                # Gzipping file
                subprocess.call(['gzip', '--best', os.path.basename(target_file)], cwd=target_dir)

                track_nb += 1
                bar.update(track_nb)
