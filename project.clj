(defproject com.rpl/rama-demo-gallery "1.0.0-SNAPSHOT"
  :source-paths ["src/main/clj"]
  :test-paths ["src/test/clj"]
  :dependencies [[com.rpl/rama-helpers "0.10.0"]
                 [org.asynchttpclient/async-http-client "2.12.3"
                  :exclusions [org.slf4j/slf4j-api ch.qos.logback/logback-classic ch.qos.logback/logback-core]]]
  :repositories [["releases" {:id "maven-releases"
                              :url "https://nexus.redplanetlabs.com/repository/maven-public-releases"}]]

  :profiles {:dev {:resource-paths ["src/test/resources/"]}
             :provided {:dependencies [[com.rpl/rama "1.3.0"]]}}
  )
