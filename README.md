# Apache Flink Table API for Java

This repository is for the [Apache Flink® Table API: Processing Data Streams in Java](https://developer.confluent.io/courses/flink-table-api-java/) course provided by Confluent Developer.

## A Simple Marketplace with Apache Flink

Throughout this course, we'll be executing a series of Flink queries focusing on an eCommerce Marketplace. Before you get started with the exercises, take a moment to familiarize yourself with the repository.

## Exercises

The course is broken down into several exercises. You will work on exercises in the `./exercises` folder.

### exercises/exercise.sh

The `exercise.sh` script is there to help you advance through the exercises. 

The basic flow of an exercise is:

- `stage` the exercise (I.E. Import any new code necessary to begin the exercise).
- `solve` the exercise (Either manually, or using the `solve` command below).

You can list the exercises by running:

```bash
./exercise.sh list
```

You can stage an exercise by running:

```bash
./exercise.sh stage <exercise number>
```

You can automatically solve an exercise by running:

```bash
./exercise.sh solve <exercise number>
```

**WARNING:** Solving an exercise will overwrite your code.

You can solve a single file by running:

```bash
./exercise.sh solve <exercise number> <file name>
```

**NOTE:** We encourage you to solve the exercise yourself. If you get stuck, you can always look at the solution in the `solutions` folder (see below).

## Staging

The `staging` folder contains the files necessary to set up each exercise. These will be copied to the `exercises` folder when you execute the `stage` command with the `exercise.sh` script.

In general, you can ignore this folder.

## Solutions

The `solutions` folder contains complete solutions for each exercise. These will be copied to the `exercises` folder when you execute the `solve` command with the `exercise.sh` script.

In general, you can ignore this folder, but you might find it helpful to reference if you get stuck.

## Development Container

This repo contains a [.devcontainer.json](.devcontainer.json) file.

If you are using a Development Container compatible IDE (Eg. VS Code, IntelliJ Ultimate) and have Docker running, you can use the `.devcontainer.json` file to automatically configure your development environment with the required settings. This includes automatically setting up the correct version of the JVM, installing Maven, installing the Confluent CLI, and potentially configuring your IDE with any required plugins. It will give you a fully functional development environment in moments.
Depending on what IDE you are using, the process can vary.

In VS Code, open the folder that contains the `.devcontainer.json` file. When prompted, re-open it in the container. Please note that you may need to install the Dev Containers plugin for VS Code prior to opening.

In IntelliJ Ultimate, right-click on the `.devcontainer.json` file, select the **Dev Containers** menu, and then choose **Create Dev Container and Mount Sources**. Once it has finished it will prompt you to connect to the container.

See here for more details:

- [Development Containers](https://containers.dev/)
- [Development Containers in VS Code](https://code.visualstudio.com/docs/devcontainers/containers)
- [Development Containers in IntelliJ](https://www.jetbrains.com/help/idea/connect-to-devcontainer.html)

If you are working in the Development Container and intend to use the Confluent CLI, you will need to login to the CLI using the [--no-browser](https://docs.confluent.io/confluent-cli/current/command-reference/confluent_login.html) flag.

## Gitpod

This repository contains a [Gitpod](https://www.gitpod.io/) configuration file. You can use Gitpod to spin up a browser-based development environment for working on these exercises.

[Open In Gitpod](https://gitpod.io/new/#https://github.com/confluentinc/learn-apache-flink-table-api-for-java-exercises)

If you are working in Gitpod and intend to use the Confluent CLI, you will need to login to the CLI using the [--no-browser](https://docs.confluent.io/confluent-cli/current/command-reference/confluent_login.html) flag.
