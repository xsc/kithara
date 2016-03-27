# kithara [![Build Status](https://travis-ci.org/xsc/kithara.svg?branch=master)](https://travis-ci.org/xsc/kithara)

__kithara__ is a (limited) [RabbitMQ][rabbitmq] client for Clojure, based on
[Lyra][lyra]. Its specific scope - and thus source of its limitations - is the
simple creation of RabbitMQ-based consumers with appropriate recovery and retry
facilities.

If you're looking for a more complete RabbitMQ library you should check out
[langohr][langohr].

[rabbitmq]: https://www.rabbitmq.com
[lyra]: https://github.com/jhalterman/lyra
[langohr]: https://github.com/michaelklishin/langohr

## Usage

Coming soon.

### Quickstart

```clojure
(require '[kithara.core :as rabbitmq]
         '[com.stuartsierra.component :as component])

(defonce rabbitmq-consumer
  (-> (rabbitmq/consumer
        (fn [{:keys [body]}]
          (println body))
        {:as :string})
      (rabbitmq/with-queue
        "my-queue"
        {:exchange     "my-exchange"
         :routing-keys ["*.print"]})
      (rabbitmq/with-channel
        {:prefetch-count 20})
      (rabbitmq/with-connection
        {:host "rabbitmq.my-cluster.com"})))

(alter-var-root #'rabbitmq-consumer component/start)
```

## Common Messaging Patterns

Kithara aims to provide easily usable implementations for common messaging
patterns and scenarios.

### Backoff via Dead-Letter-Queues

The namespace `kithara.patterns.dead-letter-backoff` contains two wrappers
`with-dead-letter-backoff` and `with-durable-dead-letter-backoff` providing
delayed requeuing of messages by dispatching them to a secondary queue, the
"dead letter queue", first. They have to be applied after `with-queue`.

The simplest version infers names of additional exchanges/queues using the
original consumer queue:

```clojure
(require '[kithara.patterns.dead-letter-backoff :as dlx])

(defonce rabbitmq-consumer-with-backoff
  (-> (rabbitmq/consumer ...)
      (dlx/with-dead-letter-backoff)
      (rabbitmq/with-queue ...)
      ...))
```

Additional options can be given - see the docstring of
`with-dead-letter-backoff` for a detailed overview.

## Lower-Level RabbitMQ API

Kithara wraps the official Java RabbitMQ client - but only as far as necessary
to build consumers (and patterns). You can access those functions using
the `kithara.rabbitmq.*` namespaces as outlined in the respective
[auto-generated documentation][rabbitmq-docs].

[rabbitmq-docs]: http://xsc.github.io/kithara/rabbitmq/index.html

## Contributing

Contributions are always welcome!

1. Create a new branch where you apply your changes (ideally also adding tests).
2. Make sure existing tests are passing.
3. Open a Pull Request on Github.

## License

```
The MIT License (MIT)

Copyright (c) 2016 Yannick Scherer

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
```
