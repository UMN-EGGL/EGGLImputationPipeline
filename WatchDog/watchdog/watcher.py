import asyncio
from asyncio.subprocess import PIPE, STDOUT


async def run_process(cmd, timeout=2):
    '''
        asyncronously run beagle and watch its output waiting for it to die
    '''
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=PIPE,
        stderr=PIPE
    ) 

    num_timeouts = 0

    while True:
        try:
            line = await asyncio.wait_for(process.stdout.readline(), timeout)
        except asyncio.TimeoutError as e:
            # Its timed out
            num_timeouts += 1
            print(f"[ WD ]: TIMED OUT READING FROM FILE, THIS IS THE {num_timeouts} TIMEOUTS")
            if num_timeouts > 5:
                process.kill()
                break
        else:
            if not line:
                break # End of File
            else:
                # Print the output
                print(f"[ WD ]: Found the output: {line.decode()}",end="",flush=True)
                continue
    return await process.wait()
            


