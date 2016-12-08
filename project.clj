(defproject kithara "0.1.8"
  :description "A Clojure Library for Reliable RabbitMQ Consumers."
  :url "https://github.com/xsc/kithara"
  :license {:name "MIT License"
            :url "https://opensource.org/licenses/MIT"
            :year 2016
            :key "mit"}
  :dependencies [[org.clojure/clojure "1.8.0" :scope "provided"]
                 [org.clojure/tools.logging "0.3.1"]
                 [net.jodah/lyra "0.5.2"]
                 [com.rabbitmq/amqp-client "3.6.1"]
                 [peripheral "0.5.2"]
                 [manifold "0.1.4"]
                 [potemkin "0.4.3"]]
  :profiles {:dev
             {:dependencies [[ch.qos.logback/logback-classic "1.1.7"]
                             [org.slf4j/slf4j-api "1.7.21"]
                             [org.clojure/test.check "0.9.0"]
                             [io.aviso/pretty "0.1.26"]]}
             :silent-test {:resource-paths ["test-resources"]}
             :codox
             [:dev
              {:plugins [[lein-codox "0.10.0"]]
               :dependencies [[codox-theme-rdash "0.1.1"]]
               :codox {:project {:name "kithara"}
                       :metadata {:doc/format :markdown}
                       :themes [:rdash]
                       :source-uri "https://github.com/xsc/kithara/blob/v{version}/{filepath}#L{line}"}}]
             :codox-consumers
             [:codox
              {:codox {:output-path "target/doc"
                       :namespaces [kithara.core
                                    kithara.config
                                    kithara.protocols
                                    #"^kithara\.patterns\.[a-z\-]+"
                                    #"^kithara\.middlewares\.[a-z\-]+"]}}]
             :codox-rabbitmq
             [:codox
              {:codox {:output-path "target/doc/rabbitmq"
                       :namespaces [kithara.config
                                    #"kithara\.rabbitmq\.[a-z\-]+"]}}]}
  :aliases {"codox"
            ["do"
             "with-profile" "+codox-consumers" "codox,"
             "with-profile" "+codox-rabbitmq" "codox"]
            "silent-test"
            ["with-profile" "+silent-test" "test"]}
  :pedantic? :abort)
