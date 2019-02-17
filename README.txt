Contrast:

C1,2   - consumers (endpoint handlers, e.g. /search)
         they handle 2 endpoints, /a and /2
B1,2   - brokers   (queue brokers)
P1,2,3 - producers (clients, sending requests to /search)

* lets assume each consumer has exactly 1 thread
* multiple brokers for HA/scale
* multiple consumers because of HA/scale
* multiple producers because of HA/scale
* assume /a is 10 times slower than /b because of business logic


         [/a, /b]                      [/a, /b]
       +----------+                +-----------+
       |          |                |           |
       |    C1    |                | C2        |
       |          |                |           |
       +-----+----+                +---//------+
             | \-                  /--/-
             |   \--           /---  /
             |      \--    /---    /-
             |         X---       /
             |     /---  \--     /
             | /---         \- /-
          +--+--+         +---X--+
          | B1  |         | B2   |
          |     |         |      |
          +--/--+         +---+\-+
            /   \-            | --\
           |      \-           \   ---\
           /        \-         |       --\
          /           \-       |          --\
         /              \-      \            ---\
        |                 \-    |                --\
        /                   \-  |                   --\
       /                      \- \                     ---\
 +----/---+                 +---\+---+              +-------------+
 |  P1    |                 |  P2    |              |   P3        |
 |        |                 |        |              |             |
 +--------+                 +--------+              +-------------+



HTTP:
In normal http world B1,2 are load balancers, and they push work to C1
and C2, and P1,2,3 are clients of the /a and /b endpoints
(https://txt.black/~jack/we-got-it-wrong)

Implementation with queues:

goal: make optimal usage of resources without oversubscribing
      and minimize the pileups of requests


the implementation is slow, but robust: the consumers poll the brokers
and they only take work when they can, the brokers return EMPTY if no
work is left

producers randomly shuffle brokers


1) producer sends request
2) broker receives it
3) puts it in in memory queue
4) consumer polls for topics it is interested int and consumes
   message by message
5) if the broker has messages it sends them to the consumer
   one by one
   otherwise EMPTY is sent and consumer polls again in 100ms


multiple concepts are introduced:

1) fat topic - consumer polls for multiple topics at the same time
   this way if consumer handles /a and /b he can poll once

   assume /a takes 10 times more than /b, usually what we do is we
   oversubscribe the threads, because in normal http mode the server
   are not in charge when the traffic will come

   instead in this system since we poll and we poll only when
   we can actually process work, we can subscribe exactly the resources
   we have.

2) no batch
   when polling we only get message by message, then ask for messages
   again until EMPTY is received. this guarantees we only get work when
   we can do it


what this system should allow you to do is also to randomly stop accepting
traffic for whatever reason (e.g. controlled System.gc()) and not care
for getting the pod out of the load balancer or anything (as long as you
pause the "get requests" polling)


Issues:

The consumers poll every 100ms, then try again to poll and if there is
something in the queue they execute it. The problem with that is that
if you have only 1 producer, it will wait for the reply before sending
a new request, so the consumer will receive "EMPTY" response for the
second poll and it will look like you can do only 1 request every 100ms
With multiple producers and multiple consumers this is not an issue.

There are many ways to solve this, such as creating second channel for
"ping", or randomizing the poll interval and adding some decay and
etc.

At the moment for every packet we set sleep = 0; and for every EMPTY
message we increase the sleep up to 100ms, this works quite nicely
and is able to achive very good speeds with only 1 producer/consumer
pair.




--------
current speed:
go consumer/producer: 1000000 messages, took: 16.37s, speed: 61085.33 per second
java consumer/producer: with the java client i can do about 80k
                        requests per second on my laptop
