# Kafka CLI Tools 

DevOps topics of Message Queue(eg kafka, pulsar, rabbitmq) like touch, rm, ls, echo, head, tail and top with ktouch, krm, kls, kecho, khead , ktail and ktop

# Why 

* super light weight, just one binary with size 5M written by rust, You can use it on mac , windows, and linux
* devops topics like files
* devops topics from diffirent message queue server with the same experiences

# Getting Started

## Create a topic 

```
ktouch localhost:9092 quickstart-topic
```

## Send a message

```
kecho localhost:9092 quickstart-topic '{"msg":"hello world!"}'
```

or with a json file

```
kecho localhost:9092 quickstart-topic -f hello_world.json
```

## Read topics with head

```
khead localhost:9092 quickstart-topic -n 100
```

## Read topics with tail


```
ktail localhost:9092 quickstart-topic -f 
```

# other kafka tools

* [kcat](https://github.com/edenhill/kcat) 
