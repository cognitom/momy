FROM node:10 AS build-env

# install tini
ENV TINI_VERSION v0.18.0
ADD https://github.com/krallin/tini/releases/download/${TINI_VERSION}/tini /tini
RUN chmod +x /tini

# install dependencies via npm
WORKDIR /app
COPY package*.json ./
RUN npm install --only=production

# multi-stage build for smaller package
FROM gcr.io/distroless/nodejs
COPY --from=build-env /tini /tini
COPY --from=build-env /app /app

# copy momy itself
COPY bin /app/bin
COPY lib /app/lib

# set working directory which would be bound to host's $PWD
WORKDIR /workdir

# wrap tini for signal handling
ENTRYPOINT ["/tini", "--", "/nodejs/bin/node", "/app/bin/momy.js"]
