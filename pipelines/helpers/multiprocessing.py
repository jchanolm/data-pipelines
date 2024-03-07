import contextlib
import multiprocessing
import os
import joblib
from tqdm import tqdm

DEBUG = os.environ.get("DEBUG", False)

class Multiprocessing:
    def __init__(self) -> None:
        self.max_thread = max(8, multiprocessing.cpu_count() * 2)
        if DEBUG:
            self.max_thread = multiprocessing.cpu_count() - 1
        os.environ["NUMEXPR_MAX_THREADS"] = str(self.max_thread)

    @contextlib.contextmanager
    def tqdm_joblib(self, tqdm_object):
        """Context manager to patch joblib to report into tqdm progress bar given as argument"""

        class TqdmBatchCompletionCallback(joblib.parallel.BatchCompletionCallBack):
            def __call__(self, *args, **kwargs):
                tqdm_object.update(n=self.batch_size)
                return super().__call__(*args, **kwargs)

        old_batch_callback = joblib.parallel.BatchCompletionCallBack
        joblib.parallel.BatchCompletionCallBack = TqdmBatchCompletionCallback
        try:
            yield tqdm_object
        finally:
            joblib.parallel.BatchCompletionCallBack = old_batch_callback
            tqdm_object.close()

    def parallel_process(self, 
                         function, 
                         array: list, 
                         description: str = "Multithreaded processing running... Give me a description!") -> list:
        """
        Wrapper to execute a function as a parallel process using jobLib. 
        It expects the following:
        - The function takes a single argument. This argument can be expanded inside the function if needed
        - The array contains the arguments for the function to be processed.py
        - The description for logging purposes

        Example:
        def square(x):
            return x**2

        Xs = [1, 2, 3, 4]

        parallel_process(square, Xs)
        >> [1, 4, 9, 16]
        """
        with self.tqdm_joblib(tqdm(desc=description, total=len(array))):
            data = joblib.Parallel(n_jobs=self.max_thread, backend="threading")(joblib.delayed(function)(element) for element in array)
        return data
