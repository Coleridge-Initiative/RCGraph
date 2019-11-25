# RCGraph

Let's manage the Rich Context knowledge graph.


## Installation

Be sure to install the dependencies:

```
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


## Testing

To run the unit tests:

```
nose2 -v --pretty-assert
```

Please create GitHub issues among the submodules for any failed tests.


## Submodules

There are GitHub repos for each entity in the KG, linked here as submodules:

  * <https://github.com/NYU-CI/RCCustomers.git>
  * <https://github.com/NYU-CI/RCDatasets.git>
  * <https://github.com/NYU-CI/RCProjects>
  * <https://github.com/NYU-CI/RCPublications.git>
  * <https://github.com/NYU-CI/RCStewards.git>

The RCLC leaderboard competition is also linked as a submodule, since
it's a consumer from this repo for its corpus updates:

  * <https://github.com/Coleridge-Initiative/rclc.git>
