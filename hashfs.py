#!/usr/bin/env python

from collections import defaultdict
from errno import ENOENT
from stat import S_IFDIR, S_IFLNK, S_IFREG
from time import time
import os

from fuse import FUSE, FuseError, FuseOperations

from hash_client import *

import logging

def find(f, seq):
    for item in seq:
        if f(item):
            return item


class MyOperations(FuseOperations):
    def __init__(self, hostname, port = 8080):
        self.factory = KeyLocatorClientFactory(hostname, port)
        self.client = HashClient(self.factory.build())
        self.fd = 0
        self.temp = {}
    
    def access(self, path, id):
        return 0

    def utimens(self, path, atime, mtime):
        dirpath = os.path.dirname(path)
        dir = self.client.get(dirpath)
        file = find(lambda x: x['name'] == os.path.basename(path), dir)
        now = time()
        file['attr']['st_atime'] = atime or now
        file['attr']['st_mtime'] = mtime or now
        self.client.put(dirpath, dir)
        return 0

    def chmod(self, path, mode):
        dirpath = os.path.dirname(path)
        dir = self.client.get(dirpath)
        file = find(lambda x: x['name'] == os.path.basename(path), dir)
        file['attr']['st_mode'] &= 0770000
        file['attr']['st_mode'] |= mode
        self.client.put(dirpath, dir)
        return 0

    def chown(self, path, uid, gid):
        dirpath = os.path.dirname(path)
        dir = self.client.get(dirpath)
        file = find(lambda x: x['name'] == os.path.basename(path), dir)
        file['attr']['st_uid'] = uid
        file['attr']['st_gid'] = gid
        self.client.put(dirpath, dir)
        return 0
    
    def create(self, path, mode):
        dirpath = os.path.dirname(path)
        dir = self.client.get(dirpath)
        dir.append(dict(name=os.path.basename(path), attr=dict(st_mode=(S_IFREG | mode), st_nlink=1,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())))
        self.client.put(dirpath, dir)
        self.client.put(path, "")
        self.fd += 1
        return self.fd
    

    def getattr(self, path, fh=None):
        if path == '/':
            st = dict(st_mode=(S_IFDIR | 0777), st_nlink=1,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time(),
                st_uid=os.getuid(), st_gid=os.getgid())
            return st

        file = None
        dir = self.client.get(os.path.dirname(path))
        if dir:
            file = find(lambda x: x['name'] == os.path.basename(path), dir)
        if not file:
            raise FuseError(ENOENT)
        st = file['attr']
        return st
    
    def init(self):
        if not self.client.get("/"):
             self.client.put("/", [])
    
    def mkdir(self, path, mode):
        dirpath = os.path.dirname(path)
        dir = self.client.get(dirpath)
        dir.append(dict(name=os.path.basename(path), attr=dict(st_mode=(S_IFDIR | mode), st_nlink=2,
                st_size=0, st_ctime=time(), st_mtime=time(), st_atime=time())))
        self.client.put(dirpath, dir)
        self.client.put(path, [])
        return 0
    
    def open(self, path, flags):
        self.fd += 1
        return self.fd
    
    def read(self, path, size, offset, fh):
        if (offset == 0):
            self.temp[path] = None
        if (not self.temp.has_key(path)):
            self.temp[path] = self.client.get(path)
        data = self.temp[path][offset:offset + size]
        return data
    
    def readdir(self, path, fh):
        result = self.client.get(path)
        result = [".", ".."] + map(lambda x: x['name'], result)
        return result
    
#    def readlink(self, path):
#        return self.data[path]
    
#    def rename(self, old, new):
#        self.files[new] = self.files.pop(old)
#        return 0
    
    def rmdir(self, path):
        return self.unlink(path)
    
#    def statvfs(self, path):
#        return dict(f_bsize=512, f_blocks=1048576, f_bavail=107374182400)
    
#    def symlink(self, source, target):
#        self.files[target] = dict(st_mode=(S_IFLNK | 0777), st_nlink=1,
#                st_size=len(source))
#        self.data[target] = source
#        return 0

    def truncate(self, path, length, fh=None):
        file_data = self.client.get(path)
        file_data = file_data[:length]
        self.client.put(path, file_data)
        dir = self.client.get(os.path.dirname(path))
        find(lambda x: x['name'] == os.path.basename(path), dir)['attr']['st_size'] = length
        self.client.put(os.path.dirname(path), dir)
        return 0
    
    def unlink(self, path):
        dir = self.client.get(os.path.dirname(path))
        self.client.remove(path)
        dir.remove(find(lambda x: x['name'] == os.path.basename(path), dir))
        self.client.put(os.path.dirname(path), dir)
        return 0
    
    
    def write(self, path, data, offset, fh):
        written_len = len(data)
        d = self.read(path, offset, 0, fh)
        data = d[:offset] + data
        self.temp[path] = data
        return written_len

    def flush(self, path, fh):
        if self.temp.has_key(path):
            data = self.temp[path]
            self.client.put(path, data)
            del self.temp[path]
            dir = self.client.get(os.path.dirname(path))
            find(lambda x: x['name'] == os.path.basename(path), dir)['attr']['st_size'] = len(data)
            self.client.put(os.path.dirname(path), dir)
            return 0

    def destroy(self):
        for item in self.temp.iterkeys():
            self.flush(item[0], None)

def main(argv):
    logging.basicConfig(level=logging.DEBUG)
    mount = argv.pop()
    port = argv.pop()
    host = argv.pop()
    fuse = FUSE(MyOperations(hostname=host, port=port), foreground=True, fsname="DonutFS", mountpoint=mount)

if __name__ == "__main__":
    main(sys.argv[1:])
