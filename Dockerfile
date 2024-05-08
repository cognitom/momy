FROM node:18 AS build-env

# install dependencies via npm
WORKDIR /app
COPY package*.json ./
RUN npm install --only=production

# multi-stage build for smaller package
FROM node:18-alpine
COPY --from=build-env /app /app

# copy momy itself
COPY bin /app/bin
COPY lib /app/lib

# set working directory which would be bound to host's $PWD
WORKDIR /workdir

# wrap tini for signal handling
ENTRYPOINT ["/usr/local/bin/node", "/app/bin/momy.js"]
