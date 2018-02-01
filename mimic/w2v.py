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
