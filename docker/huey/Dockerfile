# Huey - DuckDB Data Explorer
# https://github.com/rpbouman/huey
#
# Minimal container som servar Huey som statisk webapp

FROM nginx:alpine

# Ta bort nginx standardsida och ladda ner Huey från GitHub
# Huey's index.html ligger i src/-katalogen
RUN apk add --no-cache curl unzip && \
    rm -rf /usr/share/nginx/html/* && \
    curl -L https://github.com/rpbouman/huey/archive/refs/heads/main.zip -o /tmp/huey.zip && \
    unzip /tmp/huey.zip -d /tmp && \
    cp -r /tmp/huey-main/src/* /usr/share/nginx/html/ && \
    rm -rf /tmp/huey.zip /tmp/huey-main && \
    apk del curl unzip && \
    ls -la /usr/share/nginx/html/

# Exponera port 80
EXPOSE 80

# Nginx startar automatiskt
