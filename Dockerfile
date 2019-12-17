FROM linkageio/dockerdev:v0.2.0

USER rob 
WORKDIR .local/src
# install bcftools
RUN wget https://github.com/samtools/bcftools/releases/download/1.9/bcftools-1.9.tar.bz2
RUN tar xvvf bcftools-1.9.tar.bz2
RUN cd bcftools-1.9 && ./configure --prefix=/home/rob/.local && make && make install

# Download Beagle
RUN curl http://faculty.washington.edu/browning/beagle/beagle.21Sep19.ec3.jar -o /home/rob/.local/src/beagle.jar
ENV PATH="$PATH:/home/rob/.local/bin"
ENV BEAGLE="/home/rob/.local/src/beagle.jar"
RUN sudo apt-get install openjdk-8-jre --yes

# Download and install GATK
RUN wget https://github.com/broadinstitute/gatk/releases/download/4.1.4.1/gatk-4.1.4.1.zip
RUN sudo apt-get install unzip
RUN unzip -o gatk-4.1.4.1.zip
ENV GATK_LOCAL_JAR="/home/rob/.local/src/gatk-4.1.4.1/gatk-package-4.1.4.1-local.jar"
RUN ln -f -s /home/rob/.local/src/gatk-4.1.4.1/gatk /home/rob/.local/bin/gatk

# We need R for the VariantRecalibrator
USER root
ENV TZ=Europe/Minsk
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
RUN apt-get install r-cran-tidyverse --yes

USER rob
WORKDIR /home/rob

# Install gcloud
RUN echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list
RUN sudo apt-get install apt-transport-https ca-certificates gnupg --yes
RUN curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -
RUN sudo apt-get update && sudo apt-get install google-cloud-sdk --yes

# install python packages
RUN .conda/bin/conda install pip
RUN .conda/bin/pip install pandas
# Install snakemake 
RUN .conda/bin/conda install -c bioconda -c conda-forge snakemake --yes

# Switch back to ROB
COPY --chown=rob:rob Snakefile .

USER root
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

