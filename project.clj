(defproject kithara "0.1.0-SNAPSHOT"
  :description "A Clojure RabbitMQ client based on Lyra."
  :url "https://github.com/xsc/kithara"
  :license {:name "MIT License"
            :url "http://opensource.org/licenses/MIT"
            :year 2016
            :key "mit"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/tools.logging "0.3.1"]
                 [net.jodah/lyra "0.5.2"]
                 [com.rabbitmq/amqp-client "3.6.1" :scope "provided"]
                 [peripheral "0.5.0"]
                 [flake "0.3.1"]
                 [potemkin "0.4.3"]]
  :profiles {:dev {:dependencies [[ch.qos.logback/logback-classic "1.1.6"]
                                  [org.slf4j/slf4j-api "1.7.18"]]}}
  :pedantic? :abort)
