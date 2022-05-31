FROM fluent/fluentd:latest
USER root

ENV \
    GEMS="fluent-plugin-elasticsearch fluent-plugin-kafka"\
    DEPS="build-base ruby-dev"

RUN apk add --no-cache --update --virtual .build-deps $DEPS \
    && gem install $GEMS --no-document \
    && gem sources --clear-all \
    && apk del .build-deps \
    && rm -rf /tmp/* /var/tmp/* /usr/lib/ruby/gems/*/cache/*.gem

