(ns kithara.test
  (:require [kithara.test
             [fixtures :as fix]
             [property :as property]
             [stack :as stack]]
            [potemkin :refer [import-vars]]))

(import-vars
  [kithara.test.fixtures
   connection-config
   exchange-name
   publish!
   use-rabbitmq-fixtures]

  [kithara.test.property
   consumer-property]

  [kithara.test.stack
   stack-gen
   stack-elements])
