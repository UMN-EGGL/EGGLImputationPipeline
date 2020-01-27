
import os
import re
import logging
import asyncio
import tempfile
import subprocess
import numpy as np
from asyncio.subprocess import PIPE, STDOUT


from .exceptions import BeagleTimeoutError

logging.basicConfig()
log = logging.getLogger('Watchdog')
log.setLevel(logging.INFO)

class watcher(object):

    def __init__(
        self,
        vcf,
        out_prefix, 
        ref=None,
        window_size=0.05,
        overlap=0.005,
        nthreads=4,
        heap_size='50g',
        timeout=60, 
        check_every=5,
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
            heap_size: (default: 50g)
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
        self.cur_window_chromosome = None
        self.cur_window_start = None
        self.cur_window_end = None
        self.cur_window_num_ref = None
        
        self.cur_window_num_samples = None
        self.cur_window_num_markers = None


    @property
    def cur_window(self):
        return f"{self.cur_window_chromosome}:{self.cur_window_start}-{self.cur_window_end}"

    async def run(self):
        '''
        Run the main logic to 
        '''
        # set some starting parameters
        filtered_vcf = None 
        # Loop and try to phase
        while True:
            try:
                phase_success = await self.watch_beagle()
                if phase_success:
                    break
            except BeagleTimeoutError as e:
                # filter the current vcf
                self.filter_window()

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
        process = await asyncio.create_subprocess_exec(
            *self.beagle_command,
            stdout=PIPE,
            stderr=PIPE
        ) 
        # Monitor the STDOUT and detect a timeout 
        while True:
            try:
                line = await asyncio.wait_for(
                    process.stdout.readline(), 
                    self.check_every
                )
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
                    process.kill()
                    # Remove the temp output files
                    if os.path.exists(self.out_prefix+'.vcf.gz'):
                        os.remove(self.out_prefix+'.vcf.gz')
                    if os.path.exists(self.out_prefix+'.log'):
                        os.remove(self.out_prefix+'.log')
                    raise BeagleTimeoutError()
            else: 
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
        # wait for the child to exit
        await process.wait()
        # return the code
        return True

    def _parse_current_info(self,line):
        # Extract window infromation
        if line.startswith('Window'):
            window = re.match('Window \d+i \(([^:]+):(\d+)-(\d+)\)',line)
            self.cur_window_chromosome = window[1]
            self.cur_window_start = window[2]
            self.cur_window_end = window[3]
        elif line.startswith('Reference samples:'):
            num_samples = re.match('^Reference samples:\s+(\d+)$',line)
            self.num_reference_samples = int(num_samples[1])
        elif line.startswith('Study markers:'):
            num_markers = re.match('^Study markers:\s+(\d+)$',line)
            self.

        else:
            # The line contains no parseable information
            pass
            
        




    def filter_window(
            self,
            threshold=0.95,
            fltr_field='VQSR'
        ):
        '''
            Returns a named temp file containing the filtered VCF. 
        '''
        new_vcf = tempfile.NamedTemporaryFile('w',suffix='.vcf',delete=True) 

        log.info(f"[ WD ]: Creating new VCF: {new_vcf.name}")
        # Index if not there ---------------------------------------------
        if not os.path.exists(input_vcf+'.csi'):
            log.info(f"[ WD ]: Indexing {input_vcf}")
            cmd = f'bcftools index {input_vcf}'.split(' ')
            index = subprocess.run(
                cmd, capture_output=True
            )  
        # Print the header -----------------------------------------------
        cmd = f'bcftools view {input_vcf} -r {window_chrom}:{0}-{max(0,window_start-1)}'.split(' ')
        log.info("[ WD ]: Printing header")
        header = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        for line in header.stdout.strip().split('\n'):
            print(line, file=new_vcf, flush=True)
        # Process the window ---------------------------------------------
        cmd = f'bcftools view -H {input_vcf} -r {window_chrom}:{window_start}-{window_end}'.split(' ')
        log.info(f"[ WD ]: Processing window: {window_chrom}:{window_start}-{window_end}")
        window = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        lines =  [x for x in window.stdout.strip().split('\n')]
        log.info(f"[ WD ]: There are {len(lines)} in the window")
        # Filter out the lowest x% of scores
        scores = []
        for line in lines:
            info_fields = line.split('\t')[7].split(';') 
            for k,v in map(lambda x: x.split('='), info_fields):
                if k == fltr_field:
                    scores.append(float(v))
        quantile_cutoff = np.quantile(scores, 1-threshold)
        log.info(f"[ WD ]: Filtering out variants with {fltr_field} < {quantile_cutoff}")
        num_filtered  = 0
        for line,score in zip(lines,scores):
            if score >= quantile_cutoff:
                print(line, file=new_vcf,flush=True)
            else:
                num_filtered += 1
        log.info(f"[ WD ]: Filtered out {num_filtered} variants in window ({100*(num_filtered/len(lines)):.2f}%)")
        # Process the rest ------------------------------------------------
        cmd = f'bcftools view -H {input_vcf} -r {window_chrom}:{window_end+1}-'.split(' ')
        log.info(f"[ WD ]: Printing out rest of variants")
        header = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        for line in header.stdout.strip().split('\n'):
            print(line, file=new_vcf, flush=True)
        
        new_bgzip = tempfile.NamedTemporaryFile('w',suffix='.vcf.gz',delete=True)
        cmd = f'bcftools view {new_vcf.name} -Oz -o {new_bgzip.name}'.split(' ')
        log.info(f"[ WD ]: compressing {new_vcf.name} into {new_bgzip.name}")
        window = subprocess.run(
            cmd, capture_output=True, encoding='utf-8',text=True
        )
        log.info(f"[ WD ]: Closing {new_vcf.name}")
        new_vcf.close()

        return new_bgzip
