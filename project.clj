(defproject kithara "0.1.0-SNAPSHOT"
  :description "A Clojure RabbitMQ client based on Lyra."
  :url "https://github.com/xsc/kithara"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"
            :year 2016
            :key "mit"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.clojure/tools.logging "0.3.1"]
                 [net.jodah/lyra "0.5.2"]
                 [com.rabbitmq/amqp-client "3.6.1" :scope "provided"]
                 [peripheral "0.5.0"]
                 [flake "0.3.1"]
                 [manifold "0.1.3"]
                 [potemkin "0.4.3"]]
  :profiles {:dev
             {:dependencies [[ch.qos.logback/logback-classic "1.1.7"]
                             [org.slf4j/slf4j-api "1.7.20"]]}
             :codox
             [:dev
              {:plugins [[lein-codox "0.9.4"]]
               :codox {:project {:name "kithara"}
                       :metadata {:doc/format :markdown}
                       :source-uri "https://github.com/xsc/kithara/blob/v{version}/{filepath}#L{line}"}}]
             :codox-consumers
             [:codox
              {:codox {:output-path "doc"
                       :namespaces [kithara.core
                                    kithara.config
                                    kithara.protocols
                                    #"^kithara\.patterns\.[a-z\-]+"
                                    #"^kithara\.middlewares\.[a-z\-]+"]}}]
             :codox-rabbitmq
             [:codox
              {:codox {:output-path "doc/rabbitmq"
                       :namespaces [kithara.config
                                    #"kithara\.rabbitmq\.[a-z\-]+"]}}]}
  :aliases {"codox"
            ["do"
             "with-profile" "+codox-consumers" "codox,"
             "with-profile" "+codox-rabbitmq" "codox"]}
  :pedantic? :abort)
