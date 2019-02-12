

C1,2   - consumers (endpoint handlers, e.g. /search)
         they handle 2 endpoints, /a and /2
B1,2   - brokers   (queue brokers)
P1,2,3 - producers (clients, sending requests to /search)

* lets assume each consumer has exactly 1 thread
* multiple brokers for HA/scale
* multiple consumers because of HA/scale
* multiple producers because of HA/scale
* assume /a is 10 times slower than /b because of business logic

goal: make optimal usage of resources without oversubscribing
      and minimize the pileups of requests

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


--------
current speed:
2019/02/12 01:07:23 ... 200000 messages, took: 6.22s, speed: 32143.80 per second
