FROM node:23.11-alpine AS doosan-erp-fetch

ARG BUILD_MODE

ENV LANG='C.UTF-8' \
    LC_ALL='C.UTF-8'

RUN npm install -g npm@10.9.2

COPY package.json /app/

COPY ./env_variables/.env.${BUILD_MODE} /app/.env
RUN if [ ! -f "/app/.env" ]; then echo "Missing .env file!"; exit 1; fi

WORKDIR /app/

RUN yarn --frozen-lockfile

FROM doosan-erp-fetch AS doosan-erp-front

COPY . /app/

RUN yarn run build --mode ${BUILD_MODE}

FROM nginx:mainline-alpine
COPY nginx/default.conf /etc/nginx/conf.d/default.conf
COPY --from=doosan-erp-front /app/dist /usr/share/nginx/html