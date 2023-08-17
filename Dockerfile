FROM camunda/connectors:0.21.3
COPY target/spanner-connector-jar-with-dependencies.jar /opt/app/
ENTRYPOINT ["/start.sh"]