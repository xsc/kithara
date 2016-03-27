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
