"""
This module exposes a multithreaded cloud storage object loader
"""

import bz2
from contextlib import contextmanager
import io
import json
import logging
import os
import queue
from queue import Empty
import re
import sys
import time
import threading

import boto3
from botocore.client import Config

from google.cloud import storage

# Only allow alpha-numeric and hyphens
# for bucket names
BUCKET_RE = re.compile("^[a-zA-Z0-9\-]*$")

# All s3 and gcs paths must be alphanumeric or hyphens
PATH_RE = re.compile("^[a-zA-Z0-9\.\-_/]*$")

# Only allow s3 and gcs cloud types
CLOUD_TYPES = {
    "s3": "s3",
    "gcs": "gcs",
}


# Setup a logger for this module
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
handler.setFormatter(formatter)
logger.addHandler(handler)


@contextmanager
def context_timer(msg):
    start_load = time.time()
    try:
        yield start_load
    finally:
        # Code to release resource, e.g.:
        end_load = time.time()
        load_time = end_load - start_load
        msg += f"  Load Time: {load_time} sec"
        logger.debug(msg)


def s3_json_loader(bucket, path, region_name="us-west-2"):
    """
    Load a JSON object off of a S3 bucket and path.

    If the path ends with '.bz2', decompress the object prior to JSON
    decode.

    On any error loading from the path, return None
    """
    try:
        msg = f"Loaded s3://{bucket}/{path}."
        with context_timer(msg):
            config = Config(
                connect_timeout=10,
                region_name=region_name,
                retries={"max_attempts": 3},
            )
            conn = boto3.resource("s3", config=config)
            payload = conn.Object(bucket, path).get()["Body"].read()

            if path.endswith(".bz2"):
                payload = bz2.decompress(payload)

            json_obj = json.loads(payload.decode("utf8"))

        return json_obj
    except Exception:
        logger.exception(f"Error loading from s3://{bucket}/{path}")

    return None


def gcs_json_loader(bucket, path):
    """
    Load a JSON object off of a GCS bucket and path.

    If the path ends with '.bz2', decompress the object prior to JSON
    decode.
    """
    try:
        msg = f"Loaded gcs://{bucket}/{path}."
        with context_timer(msg):
            with io.BytesIO() as tmpfile:
                client = storage.Client()
                bucket = client.get_bucket(bucket)
                blob = bucket.blob(path)
                blob.download_to_file(tmpfile)
                tmpfile.seek(0)
                payload = tmpfile.read()
                if path.endswith(".bz2"):
                    payload = bz2.decompress(payload)
                json_obj = json.loads(payload.decode("utf8"))
                return json_obj
    except Exception:
        logger.exception(f"Error loading from gcs://{bucket}/{path}")

    return None


class Timer:
    def __init__(self, clock, ttl):
        """
        Timer that accepts a TTL in seconds
        """

        self._ttl_incr = 0
        self._clock = clock
        self._expiry_time = 0
        self._ttl = ttl

        self._start = self._clock.time()

    def has_expired(self):
        now = self._clock.time()

        future = self._start + (self._ttl_incr * self._ttl)

        expired = now > future
        logger.debug(
            f"Incr: {self._ttl_incr} Comparing time: {now} > {future} == {expired}"
        )

        if expired:
            self._ttl_incr += 1
        return expired


def refresh_thread(clock, ttl, refresh_queue):
    """
    Loop forever checking the expiry time and pushing a message into
    the refresh_queue if a model should be reloaded
    """
    t = Timer(clock, ttl)
    while True:
        if t.has_expired():
            logger.debug("Object expired - putting event onto refresh_queue")
            refresh_queue.put(True)
        time.sleep(1)


def nonblock_dequeue(refresh_queue):
    try:
        return refresh_queue.get(block=False)
    except Empty:
        return False


def result_thread(model_loader, refresh_queue, result_queue, transformer):
    while True:
        refresh_now = nonblock_dequeue(refresh_queue)
        if refresh_now is False:
            logger.debug("Refresh not required")
            time.sleep(1)
            continue

        result = model_loader()
        logger.debug("Model is loaded")
        if transformer is not None:
            logger.debug("Transform being applied")
            result = transformer(result)
        logger.debug("Final model added to result_queue")
        result_queue.put(result)


class ThreadedObjectCache:
    """
    This cache provides an interface for loading JSON objects, or
    optionally a higher level abstraction where models are loaded from
    GCS or S3.

    Three separate threads are spun up to load models.
        1. Models are loaded and transformed in a background thread.
        2. A background thread is spun up to check if the model needs to
           be refreshed.
        3. A dequeue thread to pull results from the result_queue to
           update the local cached copy

    Final computed models are pushed into a shared queue so the main
    thread does not block.

    Errors on loading models are only exposed on the initial load of
    models.  Subsequent load failures are not visible to the caller
    but are logged as exceptions to allow out of band error trapping.

    """

    def __init__(
        self,
        cloud_type,
        bucket,
        path,
        ttl=14400,  # Default to 4 hour TTL
        clock=time,
        transformer=None,
        block_until_cached=False,
    ):

        self._cloud_type = cloud_type
        self._bucket = bucket
        self._path = os.path.normpath(path)
        self._expiry_time = 0
        self._clock = clock
        self._transformer = transformer
        self._ttl = ttl

        # Setup a queue to put new JSON objects that are read in by
        # the background thread so that we don't have to wait on
        # anything
        self._result_queue = queue.Queue(10)

        # Setup a queue that signals that the object should be
        # refreshed
        self._refresh_queue = queue.Queue(10)

        # This is where we are going to store cached results
        self._cached_result = None

        assert self._cloud_type in CLOUD_TYPES
        assert BUCKET_RE.match(self._bucket)
        assert PATH_RE.match(self._path)

        # Note that the refresh thread is daemonized so that the
        # thread dies when the process dies
        self._refresh_thread = threading.Thread(
            target=refresh_thread,
            args=(self._clock, self._ttl, self._refresh_queue),
            daemon=True,
        )

        self._result_thread = threading.Thread(
            target=result_thread,
            args=(
                self.load_model,
                self._refresh_queue,
                self._result_queue,
                self._transformer,
            ),
            daemon=True,
        )

        self._update_cache_thread = threading.Thread(
            target=self._dequeue_result, args=(), daemon=True
        )

        self._refresh_thread.start()
        self._result_thread.start()
        self._update_cache_thread.start()

        if block_until_cached:
            self.block_until_cached()

    def block_until_cached(self):
        """
        Block until we have a cached result
        """
        while True:
            if self._cached_result is not None:
                break
            time.sleep(1)

    def get(self):
        return self._cached_result

    def _dequeue_result(self):
        while True:
            result = nonblock_dequeue(self._result_queue)
            if result is False:
                logger.debug("No new model for cache")
                time.sleep(1)
                continue

            assert result is not None
            # We've dequeued a result - clobber the current instance
            logger.debug("Writing new model to cache")
            self._cached_result = result
            time.sleep(1)

    def load_model(self):
        """
        This method is called by the background `result_thread` to
        load data from the cloud
        """
        if self._cloud_type == CLOUD_TYPES["s3"]:
            return s3_json_loader(self._bucket, self._path, self._transformer)
        else:
            return gcs_json_loader(self._bucket, self._path, self._transformer)
