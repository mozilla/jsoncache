"""
This module exposes a multithreaded cloud storage object loader
"""

import bz2
from contextlib import contextmanager
import io
import json
import os
import queue
from queue import Empty
import re
import time
import threading

from jsoncache.log import get_logger

import boto3
from botocore.client import Config

from google.cloud import storage

# Only allow alpha-numeric and hyphens
# for bucket names
BUCKET_RE = re.compile(r"^[a-zA-Z0-9\-]*$")

# All s3 and gcs paths must be alphanumeric or hyphens
PATH_RE = re.compile(r"[^/][a-zA-Z0-9\.\-_/]*[^/]$")

# Only allow s3 and gcs cloud types
CLOUD_TYPES = {
    "s3": "s3",
    "gcs": "gcs",
}

CHUNK_SIZE = 100 * 1000  # 100k byte reads at a time


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
        # Setup a logger for this module
        get_logger("jsoncache").info(msg)


def decode_payload(payload, path):
    """
    We need to decode the binary payload off of the storage server.

    Most of the time - we just need the binary data.

    For `.json` or `.json.bz2` files we need to do explicit decoding
    """
    if path.endswith(".bz2"):
        payload = bz2.decompress(payload)
        path = path[:-4]

    if path.endswith(".json"):
        payload = json.loads(payload.decode("utf8"))

    return payload


def s3_json_loader(bucket, path, region_name="us-west-2"):
    """
    Load a JSON object off of a S3 bucket and path.

    If the path ends with '.bz2', decompress the object prior to JSON
    decode.

    On any error loading from the path, return None
    """
    logger = get_logger("jsoncache")
    try:
        s3_fullpath = f"s3://{bucket}/{path}"
        msg = f"Loaded {s3_fullpath}"
        with context_timer(msg):
            config = Config(
                connect_timeout=10,
                region_name=region_name,
                retries={"max_attempts": 3},
            )
            s3 = boto3.resource("s3", config=config)

            bucket = s3.Bucket(bucket)
            s3_obj = bucket.Object(path).get()
            s3_stream = s3_obj["Body"]

            with io.BytesIO() as file_out:
                bytes_read = 0
                while True:
                    chunk = s3_stream.read(CHUNK_SIZE)
                    bytes_read += len(chunk)
                    if chunk:
                        logger.info(f"Read {bytes_read} bytes from {s3_fullpath}")
                        file_out.write(chunk)
                    else:
                        break

                file_out.seek(0)
                payload = file_out.read()
                payload = decode_payload(payload, path)
                return payload
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

                payload = decode_payload(payload, path)

                return payload
    except Exception:
        get_logger("jsoncache").exception(f"Error loading from gcs://{bucket}/{path}")

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
        get_logger("jsoncache").debug(
            f"Incr: {self._ttl_incr} Comparing time: {now} > {future} == {expired}"
        )

        if expired:
            self._ttl_incr += 1
        return expired


def nonblock_dequeue(refresh_queue):
    try:
        return refresh_queue.get(block=False)
    except Empty:
        return False


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
        assert BUCKET_RE.match(self._bucket), "Invalid bucket name"
        assert PATH_RE.match(self._path), "Invalid path for storage"

        # Note that the refresh thread is daemonized so that the
        # thread dies when the process dies
        self._refresh_thread = threading.Thread(
            target=self.refresh_thread,
            args=(self._clock, self._ttl, self._refresh_queue),
            daemon=True,
        )

        self._result_thread = threading.Thread(
            target=self.result_thread,
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

        self._stop = False

        self._refresh_thread.start()
        self._result_thread.start()
        self._update_cache_thread.start()

        if block_until_cached:
            self.block_until_cached()

    def stop(self):
        self._stop = True

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
        logger = get_logger("jsoncache")
        while True:
            if self._stop:
                break
            result = nonblock_dequeue(self._result_queue)
            if result is False:
                logger.debug("No new model for cache")
                time.sleep(1)
                continue

            if result is None:
                logger.error("Ignoring None dequeued to clobber cached value.")
                continue

            # We've dequeued a result - clobber the current instance
            logger.debug("Writing new model to cache")
            self._cached_result = result
            time.sleep(1)
        logger.info(f"dequeue thread stopped. thread:{threading.get_ident()}")

    def load_model(self):
        """
        This method is called by the background `result_thread` to
        load data from the cloud

        On error, this method will return None
        """
        if self._cloud_type == CLOUD_TYPES["s3"]:
            return s3_json_loader(self._bucket, self._path, self._transformer)
        else:
            return gcs_json_loader(self._bucket, self._path, self._transformer)

    def refresh_thread(self, clock, ttl, refresh_queue):
        """
        Loop forever checking the expiry time and pushing a message into
        the refresh_queue if a model should be reloaded
        """
        logger = get_logger("jsoncache")
        t = Timer(clock, ttl)
        while True:
            if self._stop:
                break
            if t.has_expired():
                logger.debug("Object expired - putting event onto refresh_queue")
                refresh_queue.put(True)
            time.sleep(1)
        logger.info(f"refresh_thread stopped. thread:{threading.get_ident()}")

    def result_thread(self, model_loader, refresh_queue, result_queue, transformer):
        logger = get_logger("jsoncache")
        while True:
            if self._stop:
                break
            refresh_now = nonblock_dequeue(refresh_queue)
            if refresh_now is False:
                logger.debug("Refresh not required")
                time.sleep(1)
                continue

            result = None
            while True:
                result = model_loader()
                # It's possible we get some kind of network failure,
                # just try again immediately
                if result is not None:
                    break
                logger.warn(
                    f"Retrying download for {self._cloud_type}://{self._bucket}/{self._path}"
                )

            logger.debug("Model is loaded")
            if transformer is not None:
                logger.debug("Transform being applied")
                result = transformer(result)
            logger.debug("Final model added to result_queue")
            result_queue.put(result)
        logger.info(f"result_thread stopped. thread:{threading.get_ident()}")
