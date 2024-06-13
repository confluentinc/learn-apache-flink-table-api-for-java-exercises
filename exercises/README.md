# A Simple Marketplace with Apache Flink

Throughout these exercises we'll be executing a series of Flink queries that focus on an eCommerce Marketplace domain. Before you get started with the exercises, take a moment to familiarize yourself with the repository.

## exercises

Throughout this course, your work will be done in the `exercises` folder. It contains an `exercise.sh` script that will help you navigate the course.

### exercises/exercise.sh

The `exercise.sh` script is there to help you advance through the exercises. At the beginning of each exercise, you can import any new code by running:

```bash
./exercise.sh stage <exercise number>
```

You can automatically solve an exercise by running:

```bash
./exercise.sh solve <exercise number>
```

**NOTE:** Solving an exercise will overwrite your code.

You can solve a single file by running:

```bash
./exercise.sh solve <exercise number> <file name>
```

**NOTE:** We encourage you to try to solve the exercise on your own. If you get stuck, you can always look at the solution in the `solutions` folder (see below).

## staging

The `staging` folder contains the files necessary to set up each exercise. These will be copied to the `exercises` folder when you execute the `stage` command with the `exercise.sh` script.

In general, you can ignore this folder.

## solutions

The `solutions` folder contains the files necessary to solve each exercise. These will be copied to the `exercises` folder when you execute the `solve` command with the `exercise.sh` script.

In general, you can ignore this folder, but you might find it helpful to reference if you get stuck.