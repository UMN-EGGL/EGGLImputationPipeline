
import os
import re
import sys
import signal
import logging
import asyncio
import tempfile
import subprocess
import numpy as np
import locuspocus as lp

from collections import defaultdict
from asyncio.subprocess import PIPE, STDOUT

from .exceptions import (
        BeagleTimeoutError, 
        BeagleHeapError
    )

logging.basicConfig()
log = logging.getLogger('Watchdog')
log.setLevel(logging.INFO)


class watcher(object):

    def __init__(
        self,
        vcf,
        out_prefix, 
        ref=None,
        window_size=0.1,
        overlap=0.01,
        nthreads=4,
        heap_size='5g',
        timeout=60, 
        check_every=1,
        fltr_field='VQSLOD',
        fltr_field_type=float,
        fltr_threshold=0.05
    ):
        '''
            Parameters
            ----------
            vcf: (pathlike string)
                input VCF file to be phased
            out_prefix: (pathlike string)
                the prefix string for the output files.
                Will produce {out_prefix}.vcf.gz and 
                {out_prefix}.log
            ref: (pathlike str to a vcf)
                The reference VCF, if set, imputation will be done.
            window_size: float
                The size of the imputation window (in megabases)
            overlap: float
                The size of the overlap window (in megabases)
            nthreads: int
                The number of threads used to phase
            timeout: int
                The number of total seconds for the function to 
                wait on a window before a timeout occurs
            check_every: int
                The number of seconds to check
            heap_size: (default: 10g)
                The size of the java heap. Passed 
                to the java -Xmx parameter.
        '''

        log.info("Creating a watcher")
        # Save the original vcf name
        self.input_vcf = vcf
        # Beagle Variables
        self.current_vcf = vcf
        self.out_prefix = out_prefix
        self.ref = ref
        self.window_size = window_size
        self.overlap = overlap
        self.nthreads = nthreads
        self.heap_size = heap_size
        try:
            self.beagle_jar = os.environ['BEAGLE_JAR']
        except KeyError:
            log.error("Please set the env variable BEAGLE_JAR to the jarfile for beagle")
            sys.exit(-1)

        # Timeout Variables 
        self.timeout = timeout
        self.check_every = check_every
        self.total_waiting = 0

        # global beagle variables
        self.num_reference_samples = None
        self.num_target_samples = None

        # Window Variables
        self.cur_window_chrom = None
        self.cur_window_start = None
        self.cur_window_end = None
        
        self.cur_window_num_ref = None
        self.cur_window_num_markers = None
        # Filter function variables
        self.fltr_field = fltr_field
        self.fltr_field_type = fltr_field_type
        self.fltr_threshold = fltr_threshold

        # Sub-process variable
        self.process = None
        signal.signal(signal.SIGINT, self._sigint_handler)

        # Store information associated with windows
        self.dropped_loci = []  

    @property
    def current_vcf(self):
        try:
            return self._current_vcf.name
        except AttributeError as e:
            return self._current_vcf

    @current_vcf.setter
    def current_vcf(self,new_value):
       self._current_vcf = new_value

    def _sigint_handler(self,sig,frame):
        '''
            What to do when we get in INTERRUPT signal
        '''
        log.info("Killing beagle")
        if self.process is not None:
            self.process.kill()
        sys.exit(1)

    @property
    def cur_window(self):
        return f"{self.cur_window_chrom}:{self.cur_window_start}-{self.cur_window_end}"

    async def run(self):
        '''
        Run the main logic to watch beagle 
        '''
        # Loop and try to phase
        while True:
            try:
                phase_success = await self.watch_beagle()
                if phase_success:
                    break
            except BeagleTimeoutError as e:
                # filter the current vcf
                self.filter_window()
            except BeagleHeapError as e:
                # increase the heap
                old_heap_size = int(self.heap_size.replace('g',''))
                self.heap_size = str(old_heap_size + 10) + 'g'


    @property
    def beagle_command(self):
        '''
            Create the command string based on the variables passed in.
        '''
        cmd = [
            'java', f'-Xmx{self.heap_size}', '-jar', self.beagle_jar,  
            f'gt={self.current_vcf}', f'out={self.out_prefix}', f'impute=true', 
            f'window={self.window_size}', f'overlap={self.overlap}',
            f'nthreads={self.nthreads}'
        ]
        # if we are imputing, insert the ref vcf
        if self.ref is not None:
            cmd.insert(5,f'ref={self.ref}')
        return cmd


    async def watch_beagle(self):
        '''
            Attempts to phase/impute a VCF file using BEAGLE.  
            
            The function monitors STDOUT and if BEAGLE stalls on producing
            output for longer than the `timeout` parameter, the process is
            killed.
        '''
        # Run the BEAGLE command in a subprocess
        log.info(f"[ WD ]: Executing the following command: {' '.join(self.beagle_command)}")
        self.process = await asyncio.create_subprocess_exec(
            *self.beagle_command,
            stdout=PIPE,
            stderr=PIPE
        ) 
        # Monitor the STDOUT and detect a timeout 
        while True:
            try:
                line = await asyncio.wait_for(
                    self.process.stdout.readline(), 
                    self.check_every
                )
                # A Line has been produced. Extract any information from it.
                if not line:
                    break # End of File
                else:
                    line = line.decode()
                    self._parse_current_info(line)
                    # reset the timeout
                    self.total_waiting = 0
                    # Print the output
                    log.info(f"[ WD ]: {line.strip()}")
                    # continue the loop
                    continue
            except asyncio.TimeoutError as e:
                # Add the total amount of time waited
                self.total_waiting += self.check_every
                log.info(
                    f"[ WD ]: TIMED OUT WAITING FOR UPDATE, "
                    f"HAVE WAITED FOR {self.total_waiting} SECONDS"
                )
                if  self.total_waiting >= self.timeout:
                    # Its timed out
                    log.info(
                        f"[ WD ]: BEAGLE TIMED OUT PROCESSING {self.cur_window}"
                    )
                    self.process.kill()
                    # Remove the temp output files
                    if os.path.exists(self.out_prefix+'.vcf.gz'):
                        os.remove(self.out_prefix+'.vcf.gz')
                    if os.path.exists(self.out_prefix+'.log'):
                        os.remove(self.out_prefix+'.log')
                    raise BeagleTimeoutError()
        # wait for the child to exit
        await self.process.wait()
        self.process = None
        # return the code
        return True

    def _parse_current_info(self,line):
        # Extract window infromation
        if line.startswith('Window'):
            window = re.match('Window \d+ \(([^:]+):(\d+)-(\d+)\)',line)
            self.cur_window_chrom = window[1]
            self.cur_window_start = int(window[2])
            self.cur_window_end = int(window[3])
        elif line.startswith('Reference samples:'):
            num_samples = re.match('^Reference samples:\s+(\d+)$',line)
            self.num_reference_samples = int(num_samples[1])
        elif line.startswith('Study markers:'):
            num_markers = re.match('^Study markers:\s+([,\d]+)$',line)
            self.cur_window_num_markers = int(num_markers[1].replace(',',''))
        elif line.startswith('ERROR: java.lang.OutOfMemoryError:'):
            raise BeagleHeapError
        else:
            # The line contains no parseable information
            pass


    def _index_current_vcf(self):
        if not os.path.exists(self.current_vcf+'.csi'):
            log.info(f"[ WD ]: Indexing {self.current_vcf}")
            cmd = f'bcftools index {self.current_vcf}'.split(' ')
            index = subprocess.run(
                cmd, capture_output=True
            )  

    async def current_vcf_lines(self,chromosome,start,stop,header=False):
        '''
            Asynchronoulsy yields lines of the current VCF file based on 
            base pair positions.
            >>> x = watcher(...)
            # get the lines 
            >>> [l async for l in x.current_vcf_lines('chr1',1,1000,header=True)]

        '''
        # In order to use bcftools, the VCF file needs to be indexed
        self._index_current_vcf()
        if not header:
            header_flag = '-H'
        else:
            header_flag = ''
        # Extract the header for the VCF file
        cmd = f'bcftools view {header_flag} {self.current_vcf} -r {chromosome}:{start}-{stop}'
        proc = await asyncio.create_subprocess_shell(
            cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        while not proc.stdout.at_eof():
            line = await proc.stdout.readline()
            yield line.decode('utf-8').strip()


    def filter_window(self):
        '''
            Returns a named temp file containing the filtered VCF. 
        '''
        filtered_vcf = tempfile.NamedTemporaryFile('w',suffix='.vcf',delete=True) 
        log.info(f"[ WD ]: Filtering VCF into: {filtered_vcf.name}")


        # Print the troublesome window into its own temp VCF
        cmd = f'bcftools view {self.current_vcf} -r {self.cur_window}'.split(' ')
        log.info(f"[ WD ]: Extracting: {self.cur_window}")
        window = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        for line in header.stdout.strip().split('\n'):
            print(line, file=filtered_vcf, flush=True)

        # Print the header -----------------------------------------------
        cmd = f'bcftools view {self.current_vcf} -r {self.cur_window_chrom}:{0}-{max(0,self.cur_window_start-1)}'.split(' ')
        log.info("[ WD ]: Printing header")
        header = subprocess.run(
            cmd, capture_output=True, encoding='utf-8', text=True
        )
        for line in header.stdout.strip().split('\n'):
            print(line, file=filtered_vcf, flush=True)
        # Process the window ---------------------------------------------
        cmd = f'bcftools view -H {self.current_vcf} -r {self.cur_window}'.split(' ')
        log.info(f"[ WD ]: Processing window: {self.cur_window}")
        window = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        lines =  [x for x in window.stdout.strip().split('\n')]
        # Filter out the lowest x% of scores
        scores = []
        for line in lines:
            info_fields = line.split('\t')[7].split(';') 
            for k,v in map(lambda x: x.split('='), info_fields):
                if k == self.fltr_field:
                    scores.append(self.fltr_field_type(v))
        # Figure out the threshold for the lowest 5%
        quantile_cutoff = np.quantile(scores, self.fltr_threshold)
        log.info(f"[ WD ]: Filtering out the bottom {self.fltr_threshold*100}% of variants")
        num_dropped = 0
        for line,score in zip(lines,scores):
            if score >= quantile_cutoff:
                # Print the vcf record into the new filtered_vcf file
                print(line, file=filtered_vcf,flush=True)
            else:
                num_dropped += 1
                # Create a locus object and add to the filtered list
                v = line.split('\t')[0:8]
                locus = lp.Locus(
                    chromosome=v[0],
                    start=int(v[1]),
                    end=int(v[1]),
                    feature_type='SNP',
                    source=self.input_vcf
                )
                # Add a name if available
                if v[2] != '.':
                    locus.name = v[2]
                # Add attrs
                for k,v in map(lambda x: x.split('='), v[7].split(';')):
                    locus[k] = v
                self.dropped_loci.append(locus)
        log.info(f"[ WD ]: Dropped a total of {num_dropped} SNPs in {self.cur_window}")

        # Process the rest ------------------------------------------------
        cmd = f'bcftools view -H {self.current_vcf} -r {self.cur_window_chrom}:{self.cur_window_end+1}-'.split(' ')
        log.info(f"[ WD ]: Printing out rest of variants")
        header = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        for line in header.stdout.strip().split('\n'):
            print(line, file=filtered_vcf, flush=True)
        
        new_bgzip = tempfile.NamedTemporaryFile('w',suffix='.vcf.gz',delete=True)
        cmd = f'bcftools view {filtered_vcf.name} -Oz -o {new_bgzip.name}'.split(' ')
        log.info(f"[ WD ]: compressing {filtered_vcf.name} into {new_bgzip.name}")
        window = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        log.info(f"[ WD ]: Closing {filtered_vcf.name}")
        filtered_vcf.close()

        self.current_vcf = new_bgzip
