FROM node:10

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
    npm-check-updates@^2.0.0 \
    istanbul@^0.4.5 \
    mocha@^3.1.2 \
    standard@^8.5.0

ENTRYPOINT ["bash", "/app/dev/entrypoint"]
