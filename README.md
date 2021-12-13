# ktools

Debug and Ops topics of Message Queue(eg kafka, pulsar, rabbitmq) like touch, rm, ls, echo, head, tail with ktouch, krm, kls, kecho, khead and ktail

# Why ktools

* super light weight, just one binary with size 5M written by rust, You can use it on mac , windows, and linux
* debug or ops topics like files
* debug or ops topocs from diffirent message queue server with the same experiences

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

