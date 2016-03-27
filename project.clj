(defproject kithara "0.1.0-SNAPSHOT"
  :description "A Clojure RabbitMQ client based on Lyra."
  :url "https://github.com/xsc/kithara"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"
            :year 2016
            :key "mit"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.clojure/tools.logging "0.3.1"]
                 [net.jodah/lyra "0.5.2"]
                 [com.rabbitmq/amqp-client "3.6.1" :scope "provided"]
                 [peripheral "0.5.0"]
                 [flake "0.3.1"]
                 [potemkin "0.4.3"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.1.6"]
                                  [org.slf4j/slf4j-api "1.7.18"]]
                   :plugins [[lein-codox "0.9.4"]]
                   :codox {:project {:name "kithara"}
                           :metadata {:doc/format :markdown}
                           :source-uri "https://github.com/xsc/kithara/blob/v{version}/{filepath}#L{line}"
                           :output-path "doc"
                           :namespaces [kithara.core
                                        kithara.config
                                        kithara.protocols
                                        #"^kithara\.patterns\.[a-z\-]+"
                                        #"^kithara\.middlewares\.[a-z\-]+"]}}}
  :pedantic? :abort)
