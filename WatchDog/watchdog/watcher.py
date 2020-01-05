import os
import re
import asyncio
import tempfile
import subprocess
import numpy as np
from asyncio.subprocess import PIPE, STDOUT


from .exceptions import PhasingTimeoutError

async def watch_beagle(
        gt,
        out_prefix,
        ref=None,
        window=0.05,
        overlap=0.005,
        nthreads=4,
        timeout=60, 
        check_every=5,
        heap_size='50g'
    ):
    # set some starting parameters
    input_vcf = gt
    filtered_vcf = None 
    # Loop and try to phase
    while True:
        try:
            # These should really be class attributes
            phase_success = await phase_vcf(
                gt=input_vcf, out_prefix=out_prefix, 
                ref=ref, window=window, overlap=overlap, 
                nthreads=nthreads, timeout=timeout,
                check_every=check_every,
                heap_size=heap_size
            )
            if phase_success:
                break
            else:
                raise 
        except PhasingTimeoutError as e:
            # filter the vcf
            filtered_vcf = filter_window(
                input_vcf,
                e.window_chrom,
                e.window_start,
                e.window_end
            )
            # This will delete the previous temp file
            try: 
                input_vcf.close()
            except AttributeError:
                pass
            input_vcf = filtered_vcf.name

async def phase_vcf(
        gt,
        out_prefix,
        ref=None,
        window=0.05,
        overlap=0.005,
        nthreads=4,
        timeout=20, 
        check_every=5,
        heap_size='50g'
    ):
    '''
        Attempts to phase a VCF file using BEAGLE. The function monitors STDOUT and
        if BEAGLE stalls on producing output for longer than the `timeout` parameter,
        the process is killed.

        Parameters
        ----------
        gt: (pathlike string)
            input VCF file to be phased
        out_prefix: (pathlike string)
            the prefix string for the output files.
            Will produce {out_prefix}.vcf.gz and 
            {out_prefix}.log
        ref: (pathlike str to a vcf)
            The reference VCF, if set, imputation will be done.
        window: float
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
    beagle_jar = os.environ['BEAGLE_JAR']
    cmd = [
        'java', f'-Xmx{heap_size}', '-jar', beagle_jar,  
        f'gt={gt}', f'out={out_prefix}', f'impute=true', 
        f'window={window}', f'overlap={overlap}',
        f'nthreads={nthreads}'
    ]
    if ref is not None:
        cmd.insert(5,f'ref={ref}')
    print(f"[ WD ]: Executing the following command: {' '.join(cmd)}")
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=PIPE,
        stderr=PIPE
    ) 

    total_waiting = 0
    return_code = 0
    window_chrom = None
    window_start = None
    window_end = None

    while True:
        try:
            line = await asyncio.wait_for(process.stdout.readline(), check_every)
        except asyncio.TimeoutError as e:
            # Its timed out
            total_waiting += check_every
            print(f"[ WD ]: TIMED OUT WAITING FOR UPDATE, WAITING FOR {total_waiting} SECONDS")
            if  total_waiting >= timeout:
                print(f"[ WD ]: BEAGLE TIMED OUT PROCESSING {window_chrom}:{window_start}-{window_end}")
                process.kill()
                # Remove the temp output files
                if os.path.exists(out_prefix+'.vcf.gz'):
                    os.remove(out_prefix+'.vcf.gz')
                if os.path.exists(out_prefix+'.log'):
                    os.remove(out_prefix+'.log')
                raise PhasingTimeoutError(
                    f"Timeout out on the window {window_chrom}:{window_start}-{window_end}",
                    window_chrom,
                    window_start,
                    window_end
                )
        else:
            if not line:
                break # End of File
            else:
                line = line.decode()
                if line.startswith('Window'):
                    window = re.match('Window \d+ \(([^:]+):(\d+)-(\d+)\)',line)
                    window_chrom,window_start,window_end = window[1],window[2],window[3]
                # reset the timeout
                total_waiting = 0
                # Print the output
                print(f"[ WD ]: {line}",end="",flush=True)
                # continue the loop
                continue
    # wait for the child to exit
    await process.wait()
    # return the code
    return True

def filter_window(
        input_vcf, 
        window_chrom, 
        window_start, 
        window_end, 
        threshold=0.95,
        fltr_field='VQSR'
    ):
    '''
    Returns a named temp file containing the filtered 
    '''
    window_start = int(window_start)
    window_end  = int(window_end)

    new_vcf = tempfile.NamedTemporaryFile('w',suffix='.vcf',delete=True) 

    print(f"[ WD ]: Creating new VCF: {new_vcf.name}")
    # Index if not there ---------------------------------------------
    if not os.path.exists(input_vcf+'.csi'):
        print(f"[ WD ]: Indexing {input_vcf}")
        cmd = f'bcftools index {input_vcf}'.split(' ')
        index = subprocess.run(
            cmd, capture_output=True
        )  
    # Print the header -----------------------------------------------
    cmd = f'bcftools view {input_vcf} -r {window_chrom}:{0}-{max(0,window_start-1)}'.split(' ')
    print("[ WD ]: Printing header")
    header = subprocess.run(
        cmd, capture_output=True, encoding='utf-8',text=True
    )
    for line in header.stdout.strip().split('\n'):
        print(line, file=new_vcf, flush=True)
    # Process the window ---------------------------------------------
    cmd = f'bcftools view -H {input_vcf} -r {window_chrom}:{window_start}-{window_end}'.split(' ')
    print(f"[ WD ]: Processing window: {window_chrom}:{window_start}-{window_end}")
    window = subprocess.run(
        cmd, capture_output=True, encoding='utf-8',text=True
    )
    lines =  [x for x in window.stdout.strip().split('\n')]
    print(f"[ WD ]: There are {len(lines)} in the window")
    # Filter out the lowest x% of scores
    breakpoint()
    scores = []
    for line in lines:
        info_fields = line.split('\t')[7].split(';') 
        for k,v in map(lambda x: x.split('='), info_fields):
            if k == fltr_field:
                scores.append(float(v))
    quantile_cutoff = np.quantile(scores, 1-threshold)
    print(f"[ WD ]: Filtering out variants with {fltr_field} < {quantile_cutoff}")
    num_filtered  = 0
    for line,score in zip(lines,scores):
        if score >= quantile_cutoff:
            print(line, file=new_vcf,flush=True)
        else:
            num_filtered += 1
    print(f"[ WD ]: Filtered out {num_filtered} variants in window ({100*(num_filtered/len(lines)):.2f}%)")
    # Process the rest ------------------------------------------------
    cmd = f'bcftools view -H {input_vcf} -r {window_chrom}:{window_end+1}-'.split(' ')
    print(f"[ WD ]: Printing out rest of variants")
    header = subprocess.run(
        cmd, capture_output=True, encoding='utf-8',text=True
    )
    for line in header.stdout.strip().split('\n'):
        print(line, file=new_vcf, flush=True)
    
    new_bgzip = tempfile.NamedTemporaryFile('w',suffix='.vcf.gz',delete=True)
    cmd = f'bcftools view {new_vcf.name} -Oz -o {new_bgzip.name}'.split(' ')
    print(f"[ WD ]: compressing {new_vcf.name} into {new_bgzip.name}")
    window = subprocess.run(
        cmd, capture_output=True, encoding='utf-8',text=True
    )
    print(f"[ WD ]: Closing {new_vcf.name}")
    new_vcf.close()

    return new_bgzip
