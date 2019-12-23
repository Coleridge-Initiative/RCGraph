# RCGraph

Manage the Rich Context knowledge graph.


## Installation

First, there are two options for creating an environment.

**Option 1:** use `virtualenv` to create a virtual environment with
the local Python 3.x as the target binary.

Then activate `virtualenv` and update your `pip` configuration:

```
source venv/bin/activate
pip install setuptools --upgrade
```

**Option 2:** use `conda` -- see
<https://docs.conda.io/projects/conda/en/latest/user-guide/install/linux.html>

Second, clone the repo:

```
git clone https://github.com/NYU-CI/RCGraph.git
```

Third, connect into the directory and initialize the local Git
configuration for the required submodules:

```
cd RCGraph
git submodule init
git submodule update
git config status.submodulesummary 1
```

Given that foundation, load the dependencies:

```
pip install -r requirements.txt
```

Fourth, set up the local `rc.cfg` configuration file and run unit the
tests (see below) to confirm that this project has been installed and
configured properly.


## Submodules

There are Git repos for almost every entity in the KG, linked into
this project as submodules:

  - <https://github.com/NYU-CI/RCCustomers>
  - <https://github.com/NYU-CI/RCDatasets>
  - <https://github.com/NYU-CI/RCHuman>
  - <https://github.com/NYU-CI/RCProjects>
  - <https://github.com/NYU-CI/RCPublications>
  - <https://github.com/NYU-CI/RCStewards>

The RCLC leaderboard competition is also linked as a submodule since
it consumes from this repo for corpus updates:

  - <https://github.com/Coleridge-Initiative/rclc.git>


## Updates

To update the submodules to their latest `HEAD` commit in `master`
branch, connect into each submodule (subdirectory) and run:

```
git fetch
git merge origin/master
```

For more info about how to use Git submodules, see:

  - <https://git-scm.com/book/en/v2/Git-Tools-Submodules>
  - <https://github.blog/2016-02-01-working-with-submodules/> 


## Workflow

### Initial Steps

  - update `datasets.json` -- datasets are the foundation for the KG
  - add a new partition of publication metadata for each data ingest


### Step 1: Graph Consistency Tests

To perform these tests:

```
nose2 -v --pretty-assert
```

Then create GitHub issues among the submodules for any failed tests.


### Step 2: Gather the DOIs, etc.

Use *title search* across the scholarly infrastructure APIs to
identify a DOI and other metadata for each publication.

```
python run_step2.py
```

Results are organized in partitions within the `bucket_stage`
subdirectory, using the same partition names from the preceding
workflow steps, to make errors easier to trace and troubleshoot.

See the `misses_step2.txt` file which reports the title of each
publication that failed every API lookup.


### Step 3: Gather the PDFs, etc.

Use *publication lookup* with DOIs across the scholarly infrastructure
APIs to identify open access PDFs, journals, authors, keywords, etc.

```
python run_step3.py
```

Results are organized in partitions in the `bucket_stage`
subdirectory, using the same partition names from the preceding
workflow steps.

See the `misses_step3.txt` file which reports the title of each
publication that failed every API lookup.


### Step 4: Reconcile Journal Entities

**This is a manual step.**

Scan results from calls to scholarly infrastructure APIs, then apply
business logic to reconcile the journal for each publication with the
`journals.json` entity listing.

```
python run_step4.py
```

Disputed entity defintions are written to standard output, and
suggested additions are written to a new `update_journals.json` file.

The person running this step must review each suggestion, then
determine whether to add the suggested journals to the `journals.json`
entities file -- or make other changes to previously described journal
entities. For example, sometimes the metadata returned from discovery
APIs has errors and would cause data quality issues within the KG.

Some good tools for manually checking journal metadata via ISSNs
include ISSN.org, Crossref, and NCBI. For example, using the ISSN
"1531-3204" to lookup journal metadata:

  - <https://portal.issn.org/api/search?search[]=MUST=allissnbis="1531-3204">
  - <http://api.crossref.org/journals/1531-3204>
  - <https://www.ncbi.nlm.nih.gov/nlmcatalog/?term=1531-3204>

Often there will be outdated/invalidated ISSNs or low-info-content
defaults (e.g., substituting SSRN) included in API results, which
could derail our KG development.

Journal names get used later in the workflow to construct UUIDs for
publications, prior to generating the public corpus. This step
performs consistency tests and filtering of the API metadata, to avoid
data quality issues later.

See the `misses_step4.txt` file which reports the title of each
publication that doesn't have a journal.

**Caveats:**

  - If you don't understand what this step performs, don't run it
  - Do not make manual edits to the `journals.json` file


### Step 5: Finalize Metadata Corrections

This workflow step finalizes the metadata corrections for each
publication, including selection of a URL, open access PDF, etc.,
along with the manual override.

```
python run_step5.py
```

Results are organized in partitions in the `bucket_final`
subdirectory, using the same partition names from the previous
workflow step.

See the `misses_step5.txt` file which reports the title of each
publication that failed every API lookup.


### Step N: Generate Corpus Update

This workflow step generates `uuid` values (late binding) for both
publications and datasets, then serializes the full output as TTL in
`tmp.ttl` and as JSON-LD in `tmp.jsonld` for a corpus update:

```
python gen_ttl.py
```

Afterwards, move the generated `tmp.*` files into the RCLC repo and
rename them:

```
mv tmp.* rclc
cd rclc
mv tmp.ttl corpus.ttl
mv tmp.jsonld corpus.jsonld
```

Then to publish the corpus:

  1. commit and create a new tagged release
  2. run `bin/download_resources.py` to download PDFs
  3. extract text from PDFs
  4. upload to the public S3 bucket and write manifest
