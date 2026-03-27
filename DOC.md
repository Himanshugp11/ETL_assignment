# SQS Data Pipeline

In this exercise, you are expected to develop a simple ETL tool. The tool should consume messages from an AWS SQS queue, 
convert events to defined structure below, then store them in database

Expectations from you are:

* Research and understand how AWS SQS works on a basic level
* Submit a working solution
* Solve problems that might occur during the setup
* Provide documentation (explained in `Documentation` section)

## Localstack
https://github.com/localstack/localstack

Instead of using an actual AWS account, you will use a localstack implementation to imitate AWS services. Localstack
uses the same API with AWS services, you can use the same API by targeting the local endpoint (stated in the
tool documentation).

## Installation Requirements
- [docker](https://www.docker.com/get-started)
- [docker-compose](https://docs.docker.com/compose/install/)

## Input

### Environment setup

In this exercise you have three files:
- `docker-compose.yml` 
- `message_generator`
- `README.md`

To setup the localstack environment, run:
```bash
$ docker-compose up
```

To setup the test case, you can run `message-generator`s appropriate for your environment. Right now darwin, linux, and
windows OSs are supported:
```bash
$ ls message-generators
darwin       linux        windows.exe
$ ./message-generators/linux    # for linux
$ ./message-generators/darwin   # for macos
```

**P.S.**: You can run `message-generator` to generate more messages.

Follow the outputs to configure your tool.


### Event structure

You will receive events with different structures. You need to convert them into below structure:

```json
{
    "id": 1,
    "mail":"aaa@gmail.com",
    "name":"AAA SSS",
    "trip": {
        "depaure": "A",
        "destination": "D",
        "start_date":"2022-10-10 12:15:00",
        "end_date": "2022-10-10 13:55:00"
    }
}
```

All values here are examples, you need to get actual values from the queue messages.

## Output

### Persisting
You need to persist the converted events. The tool should be able to run multiple times, just like any other
commandline tool. The queue should be empty after you finish processing all events.


Please setup a local database, that can be reproducible in our systems as well (both Linux and Darwin). We prefer to have a `docker run`
command to run the database of your choice.

### Language
Please prepare your solution with your language of choice. However, you are encouraged to use one of:

- Java/Kotlin/Scala
- Go
- Python 3


Please motivate your choice of programming language in the documentation.

### Documentation
You are expected to provide the source code and a documentation of the tool. Please add a `DOCUMENTATION.md` file in
you submission which includes:

- How to build the tool and build requirements
- How to configure the environment (if necessary)
- How to run the tool
- How to use the tool (options, parameters, etc.)
- Challenges while solving the problem

### Submission format
Please open a Pull/Merge request to this repository.

```
$ tree .
├── DOCUMENTATION.md
└── src
    └── ...
```
Please include DOCUMENTATION.md, source code, and build scripts if necessary to your submission.

Your submission should be able to run with a single command. You can add a `run.sh` script that runs required commands if needed. See Bonus Points #3.

--

Your program will be judged on the quality of the code as well as the correctness of the output.

## Bonus points
1. Include your database setup to `docker-compose` setup as container.
2. Make your tool runnable by docker. Provide running instructions.
3. Prepare a Makefile that builds your submission and runs it.

--
