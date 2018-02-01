import os
import random
import re

import gensim


class FilesIterator:

    def __init__(self, input_directory):

        self.input_directory = input_directory
        self.file_list = list()

        for root, dirs, files in os.walk(os.path.abspath(input_directory)):
            for filename in files:
                source_file = os.path.join(root, filename)
                self.file_list.append(source_file)

    def __iter__(self):

        random.shuffle(self.file_list)

        for filename in self.file_list:
            with open(os.path.abspath(filename), "r", encoding="UTF-8") as input_file:
                all_lines = list(input_file)
                random.shuffle(all_lines)

                for line in all_lines:
                    if re.match("^$", line):
                        continue

                    yield line.rstrip("\n").split(" ")


def build_model(input_directory, target_dir, model_prefix, size=100, window=5, min_count=5, sg=0, n_jobs=1,
                iterations=5, neg_sample=5, sample=0.001, alpha=0.025):

    target_model_name = os.path.join(target_dir, '{}.pkl'.format(model_prefix))

    sentences = FilesIterator(input_directory)

    model = gensim.models.Word2Vec(sentences, sg=sg, workers=n_jobs, iter=iterations, window=window, size=size,
                                   min_count=min_count, negative=neg_sample, sample=sample, alpha=alpha)

    model.save(target_model_name)


# def prep_w2v(input_dir, output_dir, n_jobs=1, lowercase=True, replace_digits=True):
#     """
#     Prepare mimic corpus for word2vec model learning
#     :param input_dir: path to the input documents (one sentence per line, tokens separated with spaces)
#     :param output_dir: path where output files will be created
#     :param n_jobs: number of processes to use
#     :param lowercase: flag to lowercase tokens
#     :param replace_digits: flag to replace token digits by 0
#     :return: nothing
#     """
#
#     # Path where output document will be created
#     document_output_path = os.path.join(os.path.abspath(output_dir), "documents")
#     ensure_dir(document_output_path)
#
#     logging.info("Gathering documents...")
#     processing_list = list()
#
#     # Collecting filenames
#     for root, dirs, files in os.walk(os.path.abspath(input_dir)):
#         for filename in files:
#             if re.match("^.*\.txt$", filename):
#                 subdir = remove_abs(re.sub(os.path.abspath(input_dir), "", root))
#                 processing_list.append((root, filename, subdir))
#
#     logging.info("Chunking file list")
#     file_path_chunks = _chunk_list(processing_list, n_jobs)
#
#     logging.info("Starting processing files")
#     Parallel(n_jobs=n_jobs)(delayed(_write_file)(file_path_list, document_output_path, lowercase=lowercase,
#                                                  replace_digits=replace_digits)
#                             for file_path_list in file_path_chunks)
#
#     logging.info("Done !")


# def _gather_token_count(root, filename, lowercase=True, replace_digits=True):
#     """
#     Count tokens for a given document
#     :param root: absolute path to the document
#     :param filename: document file name
#     :param lowercase: should we lowercase tokens?
#     :param replace_digits: should we replace digits by 0?
#     :return: token count dictionary
#     """
#
#     # Will contain token counts
#     document_tokens = defaultdict(int)
#
#     # File full path
#     source_txt_file = os.path.join(root, filename)
#
#     with open(source_txt_file, "r", encoding="UTF-8") as input_file:
#         for line in input_file:
#
#             if re.match("^$", line):
#                 continue
#
#             tokens = line.rstrip("\n").split(" ")
#
#             for token_str in tokens:
#
#                 if lowercase:
#                     token_str = token_str.lower()
#
#                 if replace_digits:
#                     token_str = re.sub("\d", "0", token_str)
#
#                 document_tokens[token_str] += 1
#
#     return document_tokens


# def _write_file(file_path_list, document_output_path, lowercase=True, replace_digits=True):
#     """
#     Write file to disk
#     :param file_path_list: list of files to process
#     :param document_output_path: output top directory
#     :param lowercase: flag to lowercase tokens
#     :param replace_digits: flag to replace token digits with 0
#     :return: nothing
#     """
#
#     for root, filename, subdir in file_path_list:
#
#         # Creating target directory
#         ensure_dir(os.path.join(os.path.abspath(document_output_path), subdir))
#
#         # Source and target file paths
#         source_file_path = os.path.join(root, filename)
#         target_file_path = os.path.join(os.path.abspath(document_output_path), subdir, filename)
#
#         with open(source_file_path, "r", encoding="UTF-8") as input_file:
#             with open(target_file_path, "w", encoding="UTF-8") as output_file:
#                 for line in input_file:
#
#                     # Skipping blank lines
#                     if re.match("^$", line):
#                         continue
#
#                     sent_tokens = list()
#
#                     for tok in line.rstrip("\n").split(" "):
#
#                         # Lowercasing
#                         if lowercase:
#                             tok = tok.lower()
#
#                         # Replacing digits
#                         if replace_digits:
#                             tok = re.sub("\d", "0", tok)
#
#                         sent_tokens.append(tok)
#
#                     # Writing sentence to file
#                     output_file.write("{}\n".format(" ".join(sent_tokens)))


# def _chunk_list(the_list, nb_parts):
#     """
#     Divide a list into chunk of equal size
#     :param the_list: list to chunk
#     :param nb_parts: number of chunks
#     :return: list of lists
#     """
#
#     division = len(the_list) / nb_parts
#
#     return [the_list[round(division * i):round(division * (i + 1))] for i in range(nb_parts)]
