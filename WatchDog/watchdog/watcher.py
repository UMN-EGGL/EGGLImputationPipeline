import os
import re
import asyncio
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
