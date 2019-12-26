import os
import re
import asyncio
import tempfile
from asyncio.subprocess import PIPE, STDOUT


async def watch_beagle(
        gt,
        out,
        impute='true',
        window=0.05,
        overlap=0.005,
        nthreads=4,
        timeout=30, 
        check_every=5):
    '''
        asyncronously run beagle and watch its output waiting for it to die
    '''
    beagle_jar = os.environ['BEAGLE_JAR']
    cmd = [
        'java', '-jar', beagle_jar, 
        f'gt={gt}', f'out={out}', f'impute={impute}', 
        f'window={window}', f'overlap={overlap}',
        f'nthreads={nthreads}'
    ]
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
                return_code = -9
                break
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
                print(f"[ WD ]: Found the output: {line}",end="",flush=True)
                # continue the loop
                continue
    # wait for the child to exit
    await process.wait()
    # return the code
    return return_code



def filter_window(input_vcf, window_chrom, window_start, window_end, threshold=0.95):
    import subprocess
    new_vcf = tempfile.NamedTemporaryFile('w') 
    print(f"Creating new VCF: {new_vcf.name}")
    # Print the header -----------------------------------------------
    cmd = f'bcftools view {input_vcf} -r {window_chrom}:{0}-{max(0,window_start-1)}'.split(' ')
    print(cmd)
    header = subprocess.Popen(
        cmd, stdout=subprocess.PIPE     
    )
    for line in header.communicate()[0].splitlines():
        print(line.decode(), file=new_vcf, flush=True)
    # Process the window ---------------------------------------------
    cmd = f'bcftools view -H {input_vcf} -r {window_chrom}:{window_start}-{window_end}'.split(' ')
    print(cmd)
    window = subprocess.Popen(
        cmd, stdout=subprocess.PIPE     
    )
    lines =  [x.decode() for x in window.communicate()[0].splitlines()]
    # Filter out the lowest x% of VQSLOD scores
    # [{x:y for x,y in map(lambda x: x.split('='),line.split('\t')[7].split(';'))}  for line in lines]
    breakpoint()
    x=1
