# MIMIC data resources

This repository regroups resources for [MIMIC data](https://mimic.physionet.org/) corpus processing

## 1. Requirements

* You have cloned the repository

 ```bash
 cd ~
 git clone git@github.com:jtourille/mimic-w2v-tools.git
 ```
* You have successfully downloaded mimic-iii and populated a postgres database. See the official 
[mimic-iii](https://mimic.physionet.org/) website for detailed instructions.

## 2. How to use

The steps below supposed that you are working in an empty directory.

```bash
mkdir ~/mimicdump
cd ~/mimicdump
```

### 2.1 - Extract text documents from database

Run the following command to extract the documents from the database. Adjust the parameters to your settings.

```bash
python ~/mimic-w2v-tools/main.py EXTRACT \
    --url postgresql://mimic@localhost:5432/mimic \
    --output_dir ~/mimicdump/01_extraction
```

### 2.2 - Pseudonymization

MIMIC documents have been anonymized. In this this step, we replace all placeholders with random data. 
The different lists of replacement elements are located in the `lists` directory at the root of the
repository. Further information concerning the origins of the lists is available at this location.

```bash
python ~/mimic-w2v-tools/main.py REPLACE \
    --input_dir ~/mimicdump/01_extraction \
    --output_dir ~/mimicdump/02_replace \
    --list_dir ~/w2v-tools/lists
```

### 2.3 - Process documents with CoreNLP

To process the documents with [CoreNLP](https://stanfordnlp.github.io/CoreNLP/), you must first download and install
 CoreNLP 3.8.0 by following the instructions on the official website. You must also download Java JDK 1.8+.
 
* Launch CoreNLP server.

```bash
java -mx4g -cp "*" edu.stanford.nlp.pipeline.StanfordCoreNLPServer -port 9000 -timeout 15000 -quiet
``` 

* Launch document processing.

```bash
python ~/mimic-w2v-tools/main.py CORENLP \
    --input_dir ~/mimicdump/02_replace \
    --output_dir ~/mimicdump/03_corenlp \
    --url http://localhost:9000
    [-n 10]
```
