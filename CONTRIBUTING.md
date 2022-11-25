# Contributing

Firstly, thank you for contributing! We are very grateful for your assistance in improving the Open Data Cube.

When contributing to this repository, please first discuss the change you wish to make via an issue,
Slack, or any other method with the owners of this repository before proposing a change.

We have a [code of conduct](code-of-conduct.md), so please follow it in all your interactions with the project.

## Pull Request Process

When you create a Pull Request, there is a template defined that has required information. Please complete this template to ensure that we can review the PR easily.

### Updating database table diagrams

##### Run SchemaSpy to generate database table diagrams

**Using Docker**
```
docker run -it --rm -v "$PWD:/output" --network="host" schemaspy/schemaspy:snapshot -u $DB_USERNAME -host localhost -port $DB_PORT -db $DB_DATABASE -t pgsql11 -schemas agdc -norows -noviews -pfp -imageformat svg
```

Grab the relationship diagram from agdc/diagrams/summary/relationships.real.large.svg

**If SchemaSpy is downloaded Locally**
```
java -jar schemaspy-6.1.0.jar -o . -u dra547 -host localhost -port 15432 -db datacube -t pgsql -dp postgresql-42.5.0.jar -s agdc -norows -noviews
```

Grab the relationship diagram from ./diagrams/summary/relationships.real.large.svg

## Enhancement proposals

We have a [Steering Council](https://github.com/opendatacube/datacube-core/wiki/steering-council) who discuss
and consider major enhancements, and there are a number of [current and past enhancement proposals](https://github.com/opendatacube/datacube-core/wiki/enhancement-proposals) documented.


## Links

In case you haven't found them yet, please checkout the following resources:

* [Documentation](https://datacube-core.readthedocs.io/en/latest/)
* [Slack](http://slack.opendatacube.org)
* [GIS Stack Exchange](https://gis.stackexchange.com/questions/tagged/open-data-cube).
