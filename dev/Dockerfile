FROM node:18

USER root

# Basic tools
RUN apt-get update && \
    cd /tmp && git clone https://github.com/ncopa/su-exec.git && cd su-exec && make && mv su-exec /usr/local/bin && \
    cd /usr/bin && curl https://getmic.ro | bash

# Working directory
WORKDIR /app

# Node tools
ENV NPM_CONFIG_LOGLEVEL=warn
RUN npm install --global \
    npm@10.5.1 \
    npm-check-updates@^16.14.18 \
    mocha@^10.4.0 \
    standard@^17.1.0

ENTRYPOINT ["bash", "/app/dev/entrypoint"]
