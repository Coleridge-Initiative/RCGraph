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
```

For more info about how to use Git submodules, see
<https://github.blog/2016-02-01-working-with-submodules/> 


## Testing

To run the unit tests:

```
nose2 -v --pretty-assert
```

Please create GitHub issues among the submodules for any failed tests.
