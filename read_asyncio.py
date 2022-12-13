import asyncio
import pandas as pd
import tqdm
import tqdm.asyncio
import time

limit = asyncio.Semaphore(10)


# define a coroutine for a task
async def read_single_data(file:str):
    async with limit:
        # block for a moment, simulate network io time
        await asyncio.sleep(1) 

        df = pd.read_csv(file,header=None)

        # return a value
        return df


# custom coroutine
async def read_all_data():
    tasks, files = [], []

    # create simulated files to process
    for _ in range(100):
        files.append("data0.csv")

    # create and schedule the task
    for file in files:
        task = asyncio.create_task(read_single_data(file))
        tasks.append(task)

    # show progress bar
    print("File processing going...")
    _ = [
        await f
        for f in tqdm.tqdm(asyncio.as_completed(tasks), total=len(tasks))
    ]
    
    # # wait for the task to complete.
    # await asyncio.gather(*tasks)

    # get the result
    df_res = [t.result() for t in tasks]
    print(len(df_res))


if __name__ == "__main__":
    st = time.time()
    asyncio.run(read_all_data())
    et = time.time()
    print(f"The run took {et-st} seconds")