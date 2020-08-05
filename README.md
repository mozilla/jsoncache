# jsoncache
Python cache control for cloud storage models


This library exposes a multithreaded JSON object loader that support
Amazon S3 and Google Cloud Storage.


## Why do I care?

Because loading JSON files from the cloud is more annoying than you
realize.

* Sometimes you're gonna get errors - log those errors.
* Sometimes you're going to have compressed JSON blobs because Google
  Cloud Storage has unmanageable timeouts for uploads
  (https://github.com/googleapis/python-storage/issues/74)
* You want your application to behave as if read errors from the cloud
  weren't a problem, but you want those errors to show up in logging.


## Quick Start


1. Import the ThreadedObjectCache class.
2. Instantiate it passing in the cloud type, bucket, path and time to
   live in seconds.
3. Call `.get()` on the ThreadedObjectCache instace.


You can optionally pass in a custom implementation of the `time`
module to override how `time.time()` works.

You can optionally pass in a custom callable `transformer` that will
apply the `transformer` function to the data before it's returned.
Typical use cases might involve initializing a sklearn model.

You can optionally pass in `block_until_cached`=True so that the
constructor will block until a model is loaded successfully from the
network.

All background threads are marked as daemon threads so using this code
won't cause your application to wait for thread death.

```
Python 3.7.8 | packaged by conda-forge | (default, Jul 31 2020, 02:37:09)
Type 'copyright', 'credits' or 'license' for more information
IPython 7.17.0 -- An enhanced Interactive Python. Type '?' for help.

In [1]: from jsoncache import *

In [2]: t = ThreadedObjectCache('s3', 'telemetry-parquet', 'taar/similarity/lr_curves.json', 10)

In [3]: 2020-08-05 16:07:14,369 - botocore.credentials - INFO - Found credentials in environment variables.
In [3]:

In [3]: t.get()
Out[3]:
[[0.0, [0.029045735469752962, 0.02468400347868071]],
 [0.005000778819764661, [0.029530930135620918, 0.025088940785616222]],
 ...
```
