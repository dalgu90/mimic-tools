import logging
import os
import re

from sqlalchemy import create_engine

from .tools import ensure_dir


def extract_mimic_documents(postgres_url, output_path):
    """
    Extract mimic documents from the database.
    Regroup documents according to their categories.
    :param postgres_url: database url where mimic-iii is stored
    :param output_path: path where files will be written
    :return: nothing
    """

    engine = create_engine(postgres_url)
    _ = engine.connect()

    # Counting documents in database
    document_count = engine.execute('SELECT COUNT(*) FROM mimiciii.noteevents;').scalar()
    category_count = engine.execute('SELECT COUNT(DISTINCT(category)) from mimiciii.noteevents;').scalar()

    logging.info("* Number of documents: {}".format(document_count))
    logging.info("* NUmber of categories: {}".format(category_count))

    logging.info("Starting extraction")
    logging.info("* Fetching categories")

    categories = engine.execute('SELECT category from mimiciii.noteevents GROUP BY category;')

    dir_divide = 1000

    # Process:
    # 1 - for each category, we fetch all the texts that are related to it
    for row in categories:
        category_str = row["category"].rstrip(" ")
        logging.info("* Processing: {}".format(category_str))

        # Category path
        cat_target_path = os.path.join(output_path, re.sub(" ", "_", re.sub("/", "-", category_str)))
        ensure_dir(cat_target_path)

        # Step 1
        cat_documents = engine.execute(
            "SELECT row_id, text FROM mimiciii.NOTEEVENTS as ne WHERE ne.category='{}';".format(row["category"])
        )

        for i, document in enumerate(cat_documents):
            current_dir_id = (i // dir_divide) + 1
            target_dir = os.path.join(cat_target_path, "{:04d}".format(current_dir_id))
            ensure_dir(target_dir)

            target_file = os.path.join(target_dir, "{:09d}.txt".format(document["row_id"]))

            with open(target_file, "w", encoding="UTF-8") as out:
                out.write(document["text"])
