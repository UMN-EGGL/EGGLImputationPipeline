FROM linkageio/dockerdev:v0.1.0

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


WORKDIR /home/rob

# install python packages
RUN .conda/bin/conda install pip
RUN .conda/bin/pip install pandas
# Install snakemake 
RUN .conda/bin/conda install -c bioconda -c conda-forge snakemake --yes

# Switch back to ROB
COPY Snakefile .

USER root
EXPOSE 22
CMD ["/usr/sbin/sshd", "-D"]

