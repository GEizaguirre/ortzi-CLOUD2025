import itertools
import os
import warnings
import signal
import sys
from multiprocessing import spawn, util

#from multiprocessing.shared_memory import SharedMemory
from multiprocessing import shared_memory

import pandas.core.common as com
import numpy as np
import pandas
from pandas import MultiIndex
from pandas.core.dtypes.inference import is_dict_like
from multiprocessing import resource_tracker
from functools import partial
import mmap
import os
import errno
import struct
import secrets
import types
#import _winapi


def patch_shared_memory_modules():
    pandas.DataFrame.to_records = dataframe_to_records


def dataframe_to_records(
        self, index=True, column_dtypes=None, index_dtypes=None
    ):

        if index:

            if isinstance(self.index, MultiIndex):
                # array of tuples to numpy cols. copy copy copy
                ix_vals = list(map(np.array, zip(*self.index._values)))
            else:
                # error: List item 0 has incompatible type "ArrayLike"; expected
                # "ndarray"
                ix_vals = [self.index.values]  # type: ignore[list-item]

            arrays = ix_vals + [
                np.asarray(self.iloc[:, i]) for i in range(len(self.columns))
            ]



            index_names = list(self.index.names)

            if isinstance(self.index, MultiIndex):
                index_names = com.fill_missing_names(index_names)
            elif index_names[0] is None:
                index_names = ["index"]

            names = [str(name) for name in itertools.chain(index_names, self.columns)]
        else:

            #  SUBSTITUTED
            # arrays = [np.asarray(self.iloc[:, i]) for i in range(len(self.columns))]

            arrays = []
            for i in range(len(self.columns)):
                column = self.iloc[:, i]
                if column.dtype == "string":
                    arrays.append(np.asarray(self.iloc[:, i], dtype=np.str_))
                else:
                    arrays.append(np.asarray(self.iloc[:, i]))


            names = [str(c) for c in self.columns]
            index_names = []

        index_len = len(index_names)
        formats = []

        for i, v in enumerate(arrays):
            index = i

            # When the names and arrays are collected, we
            # first collect those in the DataFrame's index,
            # followed by those in its columns.
            #
            # Thus, the total length of the array is:
            # len(index_names) + len(DataFrame.columns).
            #
            # This check allows us to see whether we are
            # handling a name / array in the index or column.
            if index < index_len:
                dtype_mapping = index_dtypes
                name = index_names[index]
            else:
                index -= index_len
                dtype_mapping = column_dtypes
                name = self.columns[index]

            # We have a dictionary, so we get the data type
            # associated with the index or column (which can
            # be denoted by its name in the DataFrame or its
            # position in DataFrame's array of indices or
            # columns, whichever is applicable.
            if is_dict_like(dtype_mapping):
                if name in dtype_mapping:
                    dtype_mapping = dtype_mapping[name]
                elif index in dtype_mapping:
                    dtype_mapping = dtype_mapping[index]
                else:
                    dtype_mapping = None

            # If no mapping can be found, use the array's
            # dtype attribute for formatting.
            #
            # A valid dtype must either be a type or
            # string naming a type.
            if dtype_mapping is None:
                formats.append(v.dtype)
            elif isinstance(dtype_mapping, (type, np.dtype, str)):
                # Argument 1 to "append" of "list" has incompatible type
                # "Union[type, dtype[Any], str]"; expected "dtype[_SCT]"  [arg-type]
                formats.append(dtype_mapping)  # type: ignore[arg-type]
            else:
                element = "row" if i < index_len else "column"
                msg = f"Invalid dtype {dtype_mapping} specified for {element} {name}"
                raise ValueError(msg)

        return np.rec.fromarrays(arrays, dtype={"names": names, "formats": formats})



"""Provides shared memory for direct access across processes.

The API of this package is currently provisional. Refer to the
documentation for details.
"""

__all__ = [ 'SharedMemory', 'ShareableList' ]


if os.name == "nt":
    import _winapi
    _USE_POSIX = False
else:
    import _posixshmem
    _USE_POSIX = True


_O_CREX = os.O_CREAT | os.O_EXCL

# FreeBSD (and perhaps other BSDs) limit names to 14 characters.
_SHM_SAFE_NAME_LENGTH = 14

# Shared memory block name prefix
if _USE_POSIX:
    _SHM_NAME_PREFIX = '/psm_'
else:
    _SHM_NAME_PREFIX = 'wnsm_'


def _make_filename():
    "Create a random filename for the shared memory object."
    # number of random bytes to use for name
    nbytes = (_SHM_SAFE_NAME_LENGTH - len(_SHM_NAME_PREFIX)) // 2
    assert nbytes >= 2, '_SHM_NAME_PREFIX too long'
    name = _SHM_NAME_PREFIX + secrets.token_hex(nbytes)
    assert len(name) <= _SHM_SAFE_NAME_LENGTH
    return name

class SeerSharedMemory:
    """Creates a new shared memory block or attaches to an existing
    shared memory block.

    Every shared memory block is assigned a unique name.  This enables
    one process to create a shared memory block with a particular name
    so that a different process can attach to that same shared memory
    block using that same name.

    As a resource for sharing data across processes, shared memory blocks
    may outlive the original process that created them.  When one process
    no longer needs access to a shared memory block that might still be
    needed by other processes, the close() method should be called.
    When a shared memory block is no longer needed by any process, the
    unlink() method should be called to ensure proper cleanup."""

    # Defaults; enables close() and unlink() to run without errors.
    _name = None
    _fd = -1
    _mmap = None
    _buf = None
    _flags = os.O_RDWR
    _mode = 0o600
    _prepend_leading_slash = True if _USE_POSIX else False

    def __init__(self, name=None, create=False, size=0):
        if not size >= 0:
            raise ValueError("'size' must be a positive integer")
        if create:
            self._flags = _O_CREX | os.O_RDWR
            if size == 0:
                raise ValueError("'size' must be a positive number different from zero")
        if name is None and not self._flags & os.O_EXCL:
            raise ValueError("'name' can only be None if create=True")

        if _USE_POSIX:

            # POSIX Shared Memory

            if name is None:
                while True:
                    name = _make_filename()
                    try:
                        self._fd = _posixshmem.shm_open(
                            name,
                            self._flags,
                            mode=self._mode
                        )
                    except FileExistsError:
                        continue
                    self._name = name
                    break
            else:
                name = "/" + name if self._prepend_leading_slash else name
                self._fd = _posixshmem.shm_open(
                    name,
                    self._flags,
                    mode=self._mode
                )
                self._name = name
            try:
                if create and size:
                    os.ftruncate(self._fd, size)
                stats = os.fstat(self._fd)
                size = stats.st_size
                self._mmap = mmap.mmap(self._fd, size)
            except OSError:
                self.unlink()
                raise

            """
            from multiprocessing.resource_tracker import register
            register(self._name, "shared_memory")
            """
            
            if create:
                from multiprocessing.resource_tracker import register
                #from .resource_tracker import register
                register(self._name, "shared_memory")
            

        else:

            # Windows Named Shared Memory

            if create:
                while True:
                    temp_name = _make_filename() if name is None else name
                    # Create and reserve shared memory block with this name
                    # until it can be attached to by mmap.
                    h_map = _winapi.CreateFileMapping(
                        _winapi.INVALID_HANDLE_VALUE,
                        _winapi.NULL,
                        _winapi.PAGE_READWRITE,
                        (size >> 32) & 0xFFFFFFFF,
                        size & 0xFFFFFFFF,
                        temp_name
                    )
                    try:
                        last_error_code = _winapi.GetLastError()
                        if last_error_code == _winapi.ERROR_ALREADY_EXISTS:
                            if name is not None:
                                raise FileExistsError(
                                    errno.EEXIST,
                                    os.strerror(errno.EEXIST),
                                    name,
                                    _winapi.ERROR_ALREADY_EXISTS
                                )
                            else:
                                continue
                        self._mmap = mmap.mmap(-1, size, tagname=temp_name)
                    finally:
                        _winapi.CloseHandle(h_map)
                    self._name = temp_name
                    break

            else:
                self._name = name
                # Dynamically determine the existing named shared memory
                # block's size which is likely a multiple of mmap.PAGESIZE.
                h_map = _winapi.OpenFileMapping(
                    _winapi.FILE_MAP_READ,
                    False,
                    name
                )
                try:
                    p_buf = _winapi.MapViewOfFile(
                        h_map,
                        _winapi.FILE_MAP_READ,
                        0,
                        0,
                        0
                    )
                finally:
                    _winapi.CloseHandle(h_map)
                size = _winapi.VirtualQuerySize(p_buf)
                self._mmap = mmap.mmap(-1, size, tagname=name)

        self._size = size
        self._buf = memoryview(self._mmap)

    def __del__(self):
        try:
            self.close()
        except OSError:
            pass

    def __reduce__(self):
        return (
            self.__class__,
            (
                self.name,
                False,
                self.size,
            ),
        )

    def __repr__(self):
        return f'{self.__class__.__name__}({self.name!r}, size={self.size})'

    @property
    def buf(self):
        "A memoryview of contents of the shared memory block."
        return self._buf

    @property
    def name(self):
        "Unique name that identifies the shared memory block."
        reported_name = self._name
        if _USE_POSIX and self._prepend_leading_slash:
            if self._name.startswith("/"):
                reported_name = self._name[1:]
        return reported_name

    @property
    def size(self):
        "Size in bytes."
        return self._size

    def close(self):
        """Closes access to the shared memory from this instance but does
        not destroy the shared memory block."""
        if self._buf is not None:
            self._buf.release()
            self._buf = None
        if self._mmap is not None:
            self._mmap.close()
            self._mmap = None
        if _USE_POSIX and self._fd >= 0:
            os.close(self._fd)
            self._fd = -1

    def unlink(self):
        """Requests that the underlying shared memory block be destroyed.

        In order to ensure proper cleanup of resources, unlink should be
        called once (and only once) across all processes which have access
        to the shared memory block."""
        if _USE_POSIX and self._name:
            from multiprocessing.resource_tracker import unregister
            #from .resource_tracker import unregister
            _posixshmem.shm_unlink(self._name)
            unregister(self._name, "shared_memory")

"""
def resource_tracker_main(fd):

    '''Run resource tracker.'''
    # protect the process from ^C and "killall python" etc
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, signal.SIG_IGN)
    if _HAVE_SIGMASK:
        signal.pthread_sigmask(signal.SIG_UNBLOCK, _IGNORED_SIGNALS)

    for f in (sys.stdin, sys.stdout):
        try:
            f.close()
        except Exception:
            pass

    cache = {rtype: set() for rtype in _CLEANUP_FUNCS.keys()}
    try:
        # keep track of registered/unregistered resources
        with open(fd, 'rb') as f:
            for line in f:
                try:
                    cmd, name, rtype = line.strip().decode('ascii').split(':')
                    cleanup_func = _CLEANUP_FUNCS.get(rtype, None)
                    if cleanup_func is None:
                        raise ValueError(
                            f'Cannot register {name} for automatic cleanup: '
                            f'unknown resource type {rtype}')

                    if cmd == 'REGISTER':
                        cache[rtype].add(name)
                    elif cmd == 'UNREGISTER':
                        cache[rtype].remove(name)
                    elif cmd == 'PROBE':
                        pass
                    else:
                        raise RuntimeError('unrecognized command %r' % cmd)
                except Exception:
                    try:
                        sys.excepthook(*sys.exc_info())
                    except:
                        pass
    finally:
        # all processes have terminated; cleanup any remaining resources
        for rtype, rtype_cache in cache.items():

            # SUBSTITUTE
            # if rtype_cache:
            if rtype_cache and all([ name.startswith("seer_shm_") for name in rtype_cache]):
                try:
                    msg = "resource_tracker: There appear to be %d leaked %s objects to clean up at shutdown" % (len(rtype_cache), rtype)
                    warnings.warn(msg)
                except Exception:
                    pass
            for name in rtype_cache:
                # For some reason the process which created and registered this
                # resource has failed to unregister it. Presumably it has
                # died.  We therefore unlink it.
                try:
                    try:


                        # SUBSTITUTE:
                        # _CLEANUP_FUNCS[rtype](name)


                        if not rtype == "shared_memory" or not name.startswith("seer_shm_"):
                            pass
                        else:
                            _CLEANUP_FUNCS[rtype](name)
                    except Exception as e:
                        warnings.warn('(%d) resource_tracker: %r: %s' % (os.getpid(), name, e))
                finally:
                    pass
"""