FROM debian:jessie
FROM python:2.7

RUN echo "\n\
deb http://mirrors.163.com/debian/ jessie main contrib non-free\n\
deb-src http://mirrors.163.com/debian jessie main contrib non-free\n\
deb http://mirrors.163.com/debian/ jessie-proposed-updates main contrib non-free\n\
deb-src http://mirrors.163.com/debian/ jessie-proposed-updates main contrib non-free\n\
deb http://mirrors.163.com/debian/ jessie-updates main contrib non-free\n\
deb-src http://mirrors.163.com/debian/ jessie-updates main contrib non-free\n\
deb http://mirrors.163.com/debian-security/ jessie/updates main contrib non-free\n\
deb-src http://mirrors.163.com/debian-security/ jessie/updates main contrib non-free\n"\
> /etc/apt/sources.list

RUN apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y -q git swig openssl libssl-dev

# Add user if uid not exist
ARG uid
ARG gid
RUN ["/bin/bash", "-c", "[[ \"`getent passwd $uid`\" ]] || groupadd -g $gid sandbox && useradd -u $uid -g sandbox sandbox -s /bin/bash"]


VOLUME ["/data", "/log"]

COPY . /opt/src
WORKDIR /opt/src
RUN make && make PREFIX=.. install && cp bin/* /usr/bin
