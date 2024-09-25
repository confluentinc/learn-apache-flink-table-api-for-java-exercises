# Apache Flink Table API for Java

This repository is for the **Apache Flink Table API for Java** course provided by Confluent Developer.

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

## Gitpod

This repository contains a [Gitpod](https://www.gitpod.io/) configuration file. You can use Gitpod to spin up a browser-based development environment for working on these exercises.