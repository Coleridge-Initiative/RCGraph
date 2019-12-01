# RCGraph

Let's manage the Rich Context knowledge graph.


## Installation

First, use `virtualenv` to create a virtual environment in the
subdirectory `venv` for Python 3.x as the target.

Then run:

```
source venv/bin/activate
pip install setuptools --upgrade
pip install -r requirements.txt
```

You'll need to set up your `rc.cfg` configuration file too.


## Updates

To update each of the submodules to their latest `master` branch
commits, be sure to run:

```
git submodule update
git pull
```

For more info about how to use Git submodules, see
<https://github.blog/2016-02-01-working-with-submodules/> 


## Submodules

There are GitHub repos for each entity in the KG, linked here as submodules:

  * <https://github.com/NYU-CI/RCCustomers>
  * <https://github.com/NYU-CI/RCDatasets>
  * <https://github.com/NYU-CI/RCHuman>
  * <https://github.com/NYU-CI/RCProjects>
  * <https://github.com/NYU-CI/RCPublications>
  * <https://github.com/NYU-CI/RCStewards>

The RCLC leaderboard competition is also linked as a submodule, since
it's a consumer from this repo for its corpus updates:

  * <https://github.com/Coleridge-Initiative/rclc.git>


## Workflow

### Step 1: Graph Consistency Tests

To run the unit tests:

```
nose2 -v --pretty-assert
```

Please create GitHub issues among the submodules for any failed tests.


### Step 2: Gather the DOI, etc.

Use title search across the scholarly infrastructure APIs to identify
a DOI and other metadata for each publication. 

```
python ./run_step2.py
```

Results are organized in partitions in the `step2` subdirectory, using
the same partition names from the previous workflow step, to make
errors easier to trace and troubleshoot.


### Step N: Generate Corpus Update

This workflow step generates `uuid` values (late binding) for both
publications and datasets, then serializes the full output as TTL in
`tmp.ttl` and as JSON-LD in `tmp.jsonld` for a corpus update:

```
python ./bin/gen_ttl.py
```

Afterwards, move/rename/test/commit the generated `tmp.*` files as a
new release for the corpus repo
<https://github.com/Coleridge-Initiative/rclc>

