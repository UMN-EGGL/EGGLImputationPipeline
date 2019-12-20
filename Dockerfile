FROM ubuntu:19.10

ENV HOME=/root
RUN mkdir -p $HOME/.local/src
WORKDIR $HOME/.local/src

RUN apt-get update && apt-get upgrade --yes
RUN apt-get install curl wget \
    openjdk-8-jre \
    gcc zlib1g-dev libbz2-dev liblzma-dev \
    build-essential \
    unzip --yes

# We need R for the VariantRecalibrator
ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install r-cran-tidyverse --yes

# install bcftools
RUN wget https://github.com/samtools/bcftools/releases/download/1.9/bcftools-1.9.tar.bz2 \
    && tar xvvf bcftools-1.9.tar.bz2 \
    && cd bcftools-1.9 \
    && ./configure --prefix=$HOME/.local && make && make install
ENV PATH="$PATH:$HOME/.local/bin"

# Download Beagle
RUN curl http://faculty.washington.edu/browning/beagle/beagle.21Sep19.ec3.jar -o $HOME/.local/src/beagle.jar
ENV BEAGLE_JAR="$HOME/.local/src/beagle.jar"

# Download and install GATK
RUN wget https://github.com/broadinstitute/gatk/releases/download/4.1.4.1/gatk-4.1.4.1.zip \
    && unzip -o gatk-4.1.4.1.zip
ENV GATK_LOCAL_JAR="$HOME/.local/src/gatk-4.1.4.1/gatk-package-4.1.4.1-local.jar"
RUN ln -f -s $HOME/.local/src/gatk-4.1.4.1/gatk $HOME/.local/bin/gatk

RUN wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh
RUN sh Miniconda3-latest-Linux-x86_64.sh -b -p $HOME/.conda
ENV PATH="$PATH:$HOME/.conda/bin"
# Install snakemake 
RUN conda install -c bioconda -c conda-forge snakemake --yes

WORKDIR $HOME
COPY  Snakefile .
