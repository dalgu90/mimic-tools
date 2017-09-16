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
### 2.2 - Regroup patient documents

Regroup documents, 1 document per patient. You can specify the number of threads you want to use (`-n` flag).

```bash
python ~/mimic-w2v-tools/main.py REGROUP \
    --input_dir ~/mimicdump/01_extraction/documents \
    --output_dir ~/mimicdump/02_regroup \
    -n 10
```

### 2.3 - Pseudonymization

MIMIC documents have been anonymized. In this this step, we replace all placeholders with random data. 
The different lists of replacement elements are located in the `lists` directory at the root of the
repository. Further information concerning the origins of the lists is available at this location.

```bash
python ~/mimic-w2v-tools/main.py REPLACE \
    --input_dir ~/mimicdump/02_regroup/documents \
    --output_dir ~/mimicdump/03_replace \
    --list_dir ~/w2v-tools/lists
```

### 2.4 - Document cleaning

Documents contain lines which are mostly composed of symbols. Also some lines start with several tabulations, spaces or
 other symbols. On top of that, sentences are cut with hard line breaks which render them difficult to process
 with NLP tools. In this step, the script will:
    1. Remove lines with a large majority of symbols
    2. Strip symbols at the beginning and at the end of the line
    3. Remove hard sentence breaks in paragraphs
    
```bash
python ~/mimic-w2v-tools/main.py CLEAN \
    --input_dir ~/mimicdump/03_replace/documents \
    --output_dir ~/mimicdump/04_clean \
    -n 10
```

### 2.5 - Process documents with cTAKES

To process the documents with [cTAKES](http://ctakes.apache.org/), you must first download and install cTAKES 3.2.2 by 
following the instructions on the official website. You must also download Java JDK 1.8+.

```bash
python ~/mimic-w2v-tools/main.py CTAKES \
    --input_dir ~/mimicdump/04_clean/documents \
    --output_dir ~/mimicdump/05_ctakes \
    --ctakes_dir /path/to/apache-ctakes-3.2.2 \
    --java_dir /path/to/jdk1.8.0_121
    --resources_dir ~/mimic-w2v-tools/resources 
```

### 2.6 - Extracted sentences and tokens

To extract the sentences and tokens from cTAKES files, run the following command:

```bash
python ~/mimic-w2v-tools/main.py CTAKES-TO-TXT \
    --input_dir ~/mimicdump/05_ctakes/documents \
    --txt_dir ~/mimicdump/04_regroup/documents \
    --output_dir ~/mimicdump/06_sent_tokens \
    -n 10
```
