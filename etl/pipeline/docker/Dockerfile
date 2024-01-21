FROM public.ecr.aws/lts/ubuntu:22.04

SHELL ["/bin/bash", "-c"]
ENV DEBIAN_FRONTEND=noninteractive
ENV TERM=xterm-256color
ENV TZ=Etc/UTC
ENV pkg='apt-get install -qqy --no-install-recommends'

RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get -qq update
RUN $pkg make python-is-python3 python3-pip python3-venv

WORKDIR /app

COPY requirements/ ./requirements/
COPY Makefile ./
RUN make env-install

COPY lib/ lib/
COPY pipeline/ pipeline/
COPY reader/ reader/

CMD ["make", "pipe"]
