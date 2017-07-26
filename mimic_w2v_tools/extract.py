import logging
import os

import progressbar
from sqlalchemy import create_engine

from .tools import ensure_dir


def extract_mimic_documents(postgres_url, output_path):
    """
    Extract mimic documents from the database. Create one directory per patient. Create one directory for 2000 patients.
    :param postgres_url: database url where mimic-iii is stored
    :param output_path: path where files will be written
    :return: nothing
    """

    engine = create_engine(postgres_url)
    _ = engine.connect()

    # Selecting all patient_id from the mimiciii.PATIENTS table
    result_subject_count = engine.execute('SELECT COUNT(*) FROM mimiciii.PATIENTS;').scalar()
    result_subject_id = engine.execute('SELECT SUBJECT_ID FROM mimiciii.PATIENTS ORDER BY SUBJECT_ID;')

    logging.info("* Number of patients: {}".format(result_subject_count))

    # Path where we store metadata
    metadata_path = os.path.join(os.path.abspath(output_path), 'metadata-mimic-3.0-1.4.txt')
    document_path = os.path.join(os.path.abspath(output_path), 'documents')

    id_dir = 1

    # Deleting PATH/FILE if they already exist
    if os.path.isdir(document_path):
        raise IsADirectoryError("{} already exists".format(document_path))

    if os.path.isfile(metadata_path):
        raise FileExistsError("{} already exists".format(metadata_path))

    ensure_dir(document_path)

    pbar = progressbar.ProgressBar(
        max_value=result_subject_count,
        widgets=[
            progressbar.Percentage(),
            ' (', progressbar.SimpleProgress(), ') ',
            progressbar.Bar(),
            ' ', progressbar.Timer(), ' ',
            progressbar.AdaptiveETA()
        ]
    )

    # Process:
    # 1 - for each patient id, we fetch all the texts that are related to it
    # 2 - we create the path where texts will be stored
    # 3 - we create the filename by concatenating subject_id, hadm_id, chartdate(YYYY-MM-DD) and row_id
    # 4 - we create the metadata line concerning the currently processed file
    # 5 - we write the line into the metadata file
    # 6 - we write the text into the text file
    with pbar as bar:
        for i, row in enumerate(result_subject_id, start=1):

            # 2000 patients per directory
            if i % 2000 == 0:
                id_dir += 1

            # Step 1
            current_documents_count = engine.execute(
                'SELECT COUNT(*) FROM mimiciii.NOTEEVENTS as ne WHERE ne.SUBJECT_ID={};'.format(row["subject_id"])).scalar()
            current_documents = engine.execute(
                'SELECT * FROM mimiciii.NOTEEVENTS as ne WHERE ne.SUBJECT_ID={};'.format(row["subject_id"]))

            logging.info("Processing patient {}/{} ({} documents)".format(
                i, result_subject_count, current_documents_count
            ))

            # Step 2
            current_output_path = os.path.join(document_path, "{:02d}".format(id_dir), "{:05d}".format(row["subject_id"]))
            ensure_dir(current_output_path)

            for document in current_documents:
                # Step 3
                current_output_filename = "{}_{}_{}_{}.txt".format(
                    document["subject_id"],
                    document["hadm_id"],
                    "{}-{:02d}-{:02d}".format(
                        document["chartdate"].year,
                        document["chartdate"].month,
                        document["chartdate"].day
                    ),
                    document["row_id"]
                )

                # Step 4
                current_metadata = ";".join([str(document[key]) for key in current_documents.keys() if key != "text"])

                # Step 5
                with open(metadata_path, "a+", encoding="UTF-8") as output_file:
                    output_file.write("{}\n".format(current_metadata))

                # Step 6
                current_output_file = os.path.join(current_output_path, current_output_filename)
                with open(current_output_file, "w", encoding="UTF-8") as output_file:
                    output_file.write(document["text"])

            bar.update(i)
