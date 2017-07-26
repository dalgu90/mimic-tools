import os
import re

from joblib import Parallel, delayed

from .tools import ensure_dir


def regroup_patient_documents(input_raw_dir, output_dir, n_jobs=1):
    """
    Regroup patient documents (one document per patient)
    :param input_raw_dir: path to the mimic extracted documents (step 1)
    :param output_dir: path where output files will be written
    :param n_jobs: number of processes to use
    :return: nothing
    """

    target_document_dir = os.path.join(os.path.abspath(output_dir), "documents")
    ensure_dir(target_document_dir)

    processing_list = list()

    for dirname in os.listdir(os.path.abspath(input_raw_dir)):
        target_dir = os.path.join(target_document_dir, dirname)
        ensure_dir(target_dir)

        for sub_dirname in os.listdir(os.path.join(input_raw_dir, dirname)):
            target_file = os.path.join(target_dir, '{}.txt'.format(sub_dirname))
            processing_list.append((os.path.join(input_raw_dir, dirname, sub_dirname), target_file))

    Parallel(n_jobs=n_jobs)(delayed(_process_patient_documents)(input_raw_dir, target_file)
                            for input_raw_dir, target_file in processing_list)


def _process_patient_documents(input_dir, target_file):
    """
    Process a patient directory
    :param input_dir: patient directory to process
    :param target_file: output directory where the file will be created
    :return: nothing
    """

    for root, dirs, files in os.walk(os.path.abspath(input_dir)):
        for filename in files:
            if re.match('^.*\.txt$', filename):
                with open(target_file, 'a+', encoding='UTF-8') as output_file:
                    output_file.write('{}\n\n'.format(
                        open(os.path.join(root, filename), 'r', encoding='UTF-8').read()
                    ))
