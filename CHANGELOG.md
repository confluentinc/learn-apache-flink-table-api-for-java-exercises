# Changelog

## Version 0.1.0

* Initial commit of required files for a public repo.

## Version 0.2.0

* Initial commit of the exercise code.

## Version 0.3.0

* Upgrade Confluent Plugin to version 1.20-42

## Version 0.3.1

* Fixing an issue with the POM file that was preventing VS Code from detecting the main method.

## Version 0.3.2

* Added a basic Gitpod configuration file.

## Version 0.3.3

* Added a dev container file.
* Updated the Gitpod configuration.

## Version 0.3.4

* Recent updates in Confluent Cloud have improved the reliability of queries. 
This negates the need for the retries that were previously used.
The retry logic has been removed as a consequence.

## Version 0.4.0

* Upgrade Confluent Plugin to version 1.20-50
* The new version of the Confluent plugin now supports `DROP TABLE`. This allows us to use it to delete temporary tables during tests, instead of needing the Kafka admin client and the Schema Registry client. This is a much simpler flow and will also simplify the exercise instructions.