import gzip
import logging
import os
import re
import shutil
import subprocess

from joblib import Parallel, delayed
from lxml import etree

from .tools import remove_abs, ensure_dir


def ctakes_corpus(corpus_path, working_dir, ctakes_dir, java_dir, resources_dir):
    """
    Process a clean mimic-iii corpus with ctakes (via remote server)
    :param corpus_path: path to the clean mimic-iii-corpus
    :param working_dir: path where gzipped json files will be created
    :param ctakes_dir: path to ctakes installation directory
    :param java_dir: path to java binaries directory
    :param resources_dir: path to resource directory where cTAKES templates are stored
    :return: nothing
    """

    # Target document path creation
    document_output_path = os.path.join(os.path.abspath(working_dir), "documents")
    log_output_path = os.path.join(os.path.abspath(working_dir), "logs")

    ensure_dir(document_output_path)
    ensure_dir(log_output_path)

    logging.info("Counting documents")

    nb_files = 0

    for root, dirs, files in os.walk(os.path.abspath(corpus_path)):
        for _ in files:
            subdir = remove_abs(re.sub(os.path.abspath(corpus_path), "", root))
            target_dir = os.path.join(os.path.abspath(document_output_path), subdir)
            ensure_dir(target_dir)
            nb_files += 1

    logging.info("* Number of files to process: {}".format(nb_files))

    source_analysis_engine = os.path.join(os.path.abspath(resources_dir), "AggregatePlaintextFastUMLSProcessor.xml")
    target_analysis_engine = os.path.join(os.path.abspath(ctakes_dir),
                                          "desc/ctakes-clinical-pipeline/desc/analysis_engine/"
                                          "custom_AggregatePlaintextFastUMLSProcessor.xml")

    shutil.copy(source_analysis_engine, target_analysis_engine)

    for dirname in os.listdir(os.path.abspath(corpus_path)):
        if os.path.isdir(os.path.join(os.path.abspath(corpus_path), dirname)):

            source_dir = os.path.join(os.path.abspath(corpus_path), dirname)
            target_dir = os.path.join(os.path.abspath(document_output_path), dirname)

            ctakes_err_log_filename = os.path.join(log_output_path, "{}-err.log".format(dirname))
            ctakes_out_log_filename = os.path.join(log_output_path, "{}-out.log".format(dirname))

            logging.info("* Processing {}".format(dirname))

            source_xml_file = os.path.join(os.path.abspath(ctakes_dir),
                                           "desc/ctakes-clinical-pipeline/desc/collection_processing_engine/"
                                           "test_plaintext.xml")
            target_xml_file = os.path.join(os.path.abspath(ctakes_dir),
                                           "desc/ctakes-clinical-pipeline/desc/collection_processing_engine/"
                                           "test_plaintext_copy.xml")

            namespaces = {
                'uima': 'http://uima.apache.org/resourceSpecifier'
            }
            tree = etree.parse(source_xml_file)
            tree.find('.//uima:nameValuePair[uima:name="InputDirectory"]/uima:value/uima:string',
                      namespaces).text = source_dir
            tree.find('.//uima:nameValuePair[uima:name="outputDir"]/uima:value/uima:string',
                      namespaces).text = target_dir
            tree.find(".//uima:casProcessor[@name='AggregatePlaintextProcessor']/uima:descriptor/uima:import",
                      namespaces).set("location", "../analysis_engine/custom_AggregatePlaintextFastUMLSProcessor.xml")

            tree.write(target_xml_file, pretty_print=True, xml_declaration=True, encoding='UTF-8')

            logging.info(" ".join(["{}/java".format(java_dir), '-Dctakes.umlsuser=julien',
                                   '-Dctakes.umlspw=sredCk7wqJAPz74k1oDR', '-cp',
                                   '{}/desc/:{}/resources/:{}/lib/*'.format(ctakes_dir, ctakes_dir, ctakes_dir),
                                   '-Dlog4j.configuration=file:{}/config/log4j.xml'.format(ctakes_dir),
                                   '-Xms1024M', '-Xmx3g', 'org.apache.ctakes.core.cpe.CmdLineCpeRunner',
                                   target_xml_file]))

            subprocess.call(["{}/java".format(java_dir), '-Dctakes.umlsuser=julien',
                             '-Dctakes.umlspw=sredCk7wqJAPz74k1oDR', '-cp',
                             '{}/desc/:{}/resources/:{}/lib/*'.format(ctakes_dir, ctakes_dir, ctakes_dir),
                             '-Dlog4j.configuration=file:{}/config/log4j.xml'.format(ctakes_dir),
                             '-Xms1024M', '-Xmx3g', 'org.apache.ctakes.ytex.tools.RunCPE',
                             target_xml_file],
                            cwd=ctakes_dir,
                            stdout=open(ctakes_out_log_filename, "w", encoding="UTF-8"),
                            stderr=open(ctakes_err_log_filename, "w", encoding="UTF-8"))

            os.remove(target_xml_file)

            for root, dirs, files in os.walk(target_dir):
                for filename in files:
                    if re.match("^.*\.xml$", filename):
                        subprocess.call(['gzip', '--best', filename], cwd=root)

    os.remove(target_analysis_engine)


def ctakes_to_txt(xml_path, txt_path, output_path, n_jobs=1):

    document_output_path = os.path.join(os.path.abspath(output_path), "documents")
    ensure_dir(document_output_path)

    logging.info("* Compiling processing list")

    processing_list = list()
    # Collecting filenames
    for root, dirs, files in os.walk(os.path.abspath(xml_path)):
        for filename in files:
            if re.match("^.*\.xml.gz$", filename):
                subdir = remove_abs(re.sub(os.path.abspath(xml_path), "", root))
                processing_list.append((root, filename, subdir))

    logging.info("* Number of files to process: {}".format(len(processing_list)))

    logging.info("* Starting extraction with {} processes".format(n_jobs))

    Parallel(n_jobs=n_jobs)(delayed(_convert_ctakes_file)(root, filename, subdir, os.path.abspath(document_output_path),
                                                          txt_path)
                            for root, filename, subdir in processing_list)

    logging.info("Done !")


def _convert_ctakes_file(root, filename, subdir, document_output_path, txt_path):

    target_path = os.path.join(document_output_path, subdir)
    ensure_dir(target_path)

    source_xml_filename = os.path.join(root, filename)

    source_txt_filename = os.path.join(txt_path, subdir, "{}.txt".format(filename.split(".")[0]))
    target_txt_filename = os.path.join(target_path, "{}.txt".format(filename.split(".")[0]))

    sentences = extract_sentences(source_txt_filename, source_xml_filename)

    with open(target_txt_filename, "w", encoding="UTF-8") as output_file:
        for sentence in sorted(sentences, key=lambda x: x["begin"]):
            sentence_str = " ".join([token["text"] for token in sentence["tokens"]])
            output_file.write("{}\n".format(sentence_str))


def extract_sentences(txt_file_path, xml_file_path):

    content_txt = open(os.path.abspath(txt_file_path), "r", encoding="UTF-8").read()
    content_xml = gzip.open(xml_file_path, 'rt', encoding='UTF-8')

    xml_tree = etree.parse(content_xml)
    xml_root = xml_tree.getroot()

    all_sentences = []

    sentences = xml_root.findall(".//org.apache.ctakes.typesystem.type.textspan.Sentence")

    for sentence in sentences:
        current_sentence = {
            "begin": int(sentence.get("begin")),
            "end": int(sentence.get("end")),
            "tokens": []
        }
        all_sentences.append(current_sentence)

    punct_tokens = xml_root.findall(".//org.apache.ctakes.typesystem.type.syntax.PunctuationToken")

    for item in punct_tokens:

        begin = int(item.get("begin"))
        end = int(item.get("end"))
        text = content_txt[int(item.get("begin")): int(item.get("end"))]

        for sentence in all_sentences:

            if sentence["begin"] <= begin < sentence["end"]:
                sentence["tokens"].append({
                    "begin": begin,
                    "end": end,
                    "text": text,
                })
                break

    word_tokens = xml_root.findall(".//org.apache.ctakes.typesystem.type.syntax.WordToken")

    for item in word_tokens:

        begin = int(item.get("begin"))
        end = int(item.get("end"))
        text = content_txt[int(item.get("begin")): int(item.get("end"))]

        for sentence in all_sentences:

            if sentence["begin"] <= begin < sentence["end"]:
                sentence["tokens"].append({
                    "begin": begin,
                    "end": end,
                    "text": text,
                })
                break

    symbol_tokens = xml_root.findall(".//org.apache.ctakes.typesystem.type.syntax.SymbolToken")

    for item in symbol_tokens:

        begin = int(item.get("begin"))
        end = int(item.get("end"))
        text = content_txt[int(item.get("begin")): int(item.get("end"))]

        for sentence in all_sentences:

            if sentence["begin"] <= begin < sentence["end"]:
                sentence["tokens"].append({
                    "begin": begin,
                    "end": end,
                    "text": text,
                })
                break

    contraction_tokens = xml_root.findall(".//org.apache.ctakes.typesystem.type.syntax.ContractionToken")

    for item in contraction_tokens:

        begin = int(item.get("begin"))
        end = int(item.get("end"))
        text = content_txt[int(item.get("begin")): int(item.get("end"))]

        for sentence in all_sentences:

            if sentence["begin"] <= begin < sentence["end"]:
                sentence["tokens"].append({
                    "begin": begin,
                    "end": end,
                    "text": text,
                })
                break

    num_tokens = xml_root.findall(".//org.apache.ctakes.typesystem.type.syntax.NumToken")

    for item in num_tokens:

        begin = int(item.get("begin"))
        end = int(item.get("end"))
        text = content_txt[int(item.get("begin")): int(item.get("end"))]

        for sentence in all_sentences:

            if sentence["begin"] <= begin < sentence["end"]:
                sentence["tokens"].append({
                    "begin": begin,
                    "end": end,
                    "text": text,
                })
                break

    for sentence in all_sentences:
        sentence["tokens"] = sorted(sentence["tokens"], key=lambda x: x["begin"])

    return all_sentences
