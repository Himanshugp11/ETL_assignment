# SQS Data Pipeline - For Admins

This exercise is a home assignment. It aims to evaluate following skills/traits:

- Ability to untar given compressed file
- Ability to search for API specifications in AWS, service functionalities
- Converting multiple sources into one structure
- Ability to use docker, bash, and one of the languages we use (Java, Kotlin, Go, Python)
- Ability to combine cloud service functionalities with a backend code and database

Please see `DOC.md` for candidate instructions.

## How it works
Candidate is expected to run `docker-compose` to boot a `localstack` container, which contains a mock SQS service.
After that, candidate is supposed to run `message_generator` to add messages to the queue. Then candidate is expected
to find out how localstack works, how SQS works, how a message can be received, how is it deleted, and how to store the data.

## How to build
Run `prepare.sh` which generates a `tar.xz` file. It contains necessary files to setup local system & go. It doesnot
add the source code, instead builds an executable for Linux, macOS, and Windows and puts them in the compressed `tar.xz` file.

## Evaluation
- Correctness: convert different sources into one single model
- Code quality
- Database choice
- Documentation quality
- Bonus: adding database to docker-compose
- Bonus: instructions/documentation of how to run
