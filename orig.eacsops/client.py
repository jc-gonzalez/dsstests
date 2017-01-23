#!/usr/bin/env python
__version__ = '@(#)$Revision: 1.26 $'

import errno, hashlib, httplib, os, pickle, random, signal, socket, ssl, stat, string, sys, time, traceback, urllib, urlparse, uuid

try:
    from globals import VT
    VT[__file__] = __version__
except:
    pass
try:
    from common.config.Environment import Env
except:
    Env = os.environ
try:
     from common.log.Message import Message
except:
     def Message(s):
         print(s)
try:
    from common.net.gpg import gpgencrypt
except:
    def gpgencrypt(*arglist, **argdict):
        print('gpgencrypt: No gpgencryption possible!')
        return True

from pdb import set_trace as breakpoint

Compressed_Exts = [
    '',                             # extensions for decompressed,
    '.gz',                          # gzip compressed
    '.bz2',                         # and bzip2 compressed
]
Compressors = {
    '':         '',
    '.gz':      'gzip --to-stdout --no-name ',
    '.bz2':     'bzip2 --stdout --compress ',
}
Decompressors = {
    '':         '',
    '.gz':      'gzip --to-stdout --decompress ',
    '.bz2':     'bzip2 --stdout --decompress ',
}

MapAction={
    'DSSGET' : 'GET',
    'DSSGETANY': 'GET',
    'DSSSTORE' :  'POST',
    'DSSSTORED':  'GET',
    'DSSCACHEFILE': 'GET',
    'DSSGETLOG': 'GET',
    'GETLOCALSTATS': 'GET',
    'GETGROUPSTATS': 'GET',
    'GETTOTALSTATS': 'GET',
    'LOCATE' : 'GET',
    'LOCATEFILE': 'GET',
    'LOCATELOCAL': 'GET',
    'LOCATEREMOTE' : 'GET',
    'MD5SUM' : 'GET',
    'PING' : 'GET',
    'SIZE' : 'GET',
    'STAT' : 'GET',
    'GET' : 'GET',
    'GETANY' : 'GET',
    'GETEXACT': 'GET',
    'GETLOCAL': 'GET',
    'GETREMOTE': 'GET',
    'STORE' : 'POST',
    'STORED' : 'POST',
    'HEAD' : 'GET',
    'HEADLOCAL' : 'GET',
    'DELETE': 'GET',
    'TAKEOVER': 'GET',
    'TESTCACHE':'GET',
    'TESTSTORE':'GET',
    'TESTFILE':'GET',
    'REGISTER':'GET',
    'RELEASE':'GET',
    'RELOAD':'GET',
}



# Trick(y)
if (not hasattr(os.path, 'sep')):
    os.path.sep = '/'
    print('Warning: Use at least python version 2.3')

class Data_IO:
    '''Class to provide data transfer between the server and client.
    '''
    def myprint(self, level, text):
        self.textbuf.append(text)
        if level and not (self.debug & level): return
        Message(text)

    def __del__(self):
        if self.status and self.textbuf:
            for text in self.textbuf: Message('DATA_IO: '+text)

    def __init__(self, host, port=None, store_host=None, store_port=None, debug=0, timeout=None, data_path='', user_id='', sleep=30.0, dstid='', secure=True, certfile=''):
        self.textbuf = []
        self._host = ''
        self._port = 0
        if dstid:
            self.dstid = dstid
        else:
            self.dstid = '%s' % (uuid.uuid4())
        self.version = __version__.split()[1]
        self.open = 0
        self.result = 0
        if type(host) == type(()):
            self.host = host[0]
            self.port = host[1]
        elif port:
            self.host = host
            self.port = int(port)
        else:
            self.host, port = host.split(':')
            self.port = int(port)
        self.original_host = self.host
        self.allhosts = []
        for h in self.original_host.split(','):
            self.allhosts += socket.gethostbyname_ex(h)[2]
        self.host2 = ''
        self.port2 = 0
        self.timeout = timeout
        self.store_port = self.port
        self.store_host = self.host
        if store_port:
            self.store_port = int(store_port)
            if store_host: self.store_host = store_host
        elif store_host:
            hp = storehost.split(':')
            if len(hp) > 1: self.store_port = int(hp[1])
        self.f_extra = None
        self.server_id = ''
        self.server_caps = ''
        self.status = 0
        self.debug = debug
        self.autoclose = True
        self.buffer = ''
        self.buffer_length = 0
        self.read_buffer_size = 16 * 1024
        self.write_buffer_size = 16 * 1024
        self.action = "NONE"
        self.bcount = 0L
        self.getpath = None
        self.cmd = None
        self.content_length = 0
        if data_path and os.path.isdir(data_path):
            self.data_path = data_path
        else:
            self.data_path = None
        self.storecheck = False
        self.storekey = None
        if user_id:
            self.user_id = user_id
        elif 'database_user' in Env.keys():
            self.user_id = Env['database_user']
        else:
            try :
                import pwd
                self.user_id = pwd.getpwuid(os.getuid())[0]
            except:
                self.user_id = '?'
        self.machine_id = socket.getfqdn()
        self.chksum = False
        self.md5sum = None
        self.fdatapath = None
        self.sleep = sleep
        self.certfile = certfile
        if certfile:
            self.secure = True
        else:
            self.certfile = None
            self.secure = secure
        self.secure2 = self.secure
        self.Secure = None
        self.store_secure = self.secure
        self.sendheader = {}
        self.recvheader = {}
        self.nextsend = False
        self.response = None
        self.jobid = None
        self.password=''

    def _set_data_path(self, data_path=None):
        if not data_path and 'data_path' in Env.keys() and Env['data_path']:
            data_path = Env['data_path']
        if data_path and os.path.isdir(data_path):
            self.data_path = data_path

    def __del__(self):

        if self.open:
            self.f.close()
        self.open = 0

    def _show(self):

        print('_show: self.version             : %s' % self.version)
        print('_show: self.open                : %d' % self.open)
        print('_show: self.result              : %d' % self.result)
        print('_show: self.host                : %s' % self.host)
        print('_show: self.port                : %d' % self.port)
        print('_show: self.secure              : %d' % self.secure)
        print('_show: self.host2               : %s' % self.host2)
        print('_show: self.port2               : %d' % self.port2)
        print('_show: self.secure2             : %d' % self.secure2)
        print('_show: self.timeout             : %f' % self.timeout)
        print('_show: self.store_host          : %s' % self.store_host)
        print('_show: self.store_port          : %d' % self.store_port)
        print('_show: self.store_secure        : %d' % self.store_secure)
        print('_show: self.f_extra             : %d' % self.f_extra)
        print('_show: self.server_id           : %s' % self.server_id)
        print('_show: self.server_caps         : %s' % self.server_caps)
        print('_show: self.status              : %d' % self.status)
        print('_show: self.debug               : %d' % self.debug)
        print('_show: self.autoclose           : %d' % self.autoclose)
        print('_show: self.buffer              : %s' % self.buffer)
        print('_show: self.buffer_length       : %d' % self.buffer_length)
        print('_show: self.read_buffer_size    : %d' % self.read_buffer_size)
        print('_show: self.write_buffer_size   : %d' % self.write_buffer_size)
        print('_show: self.recvheader          : %s' % self.recvheader)
        print('_show: self.sendheader          : %s' % self.sendheader)
        print('_show: self.action              : %s' % self.action)
        print('_show: self.bcount              : %d' % self.bcount)
        print('_show: self.getpath             : %s' % self.getpath)
        print('_show: self.content_length      : %d' % self.content_length)
        print('_show: self.data_path           : %s' % self.data_path)
        print('_show: self.user_id             : %s' % self.user_id)
        print('_show: self.machine_id          : %s' % self.machine_id)

    def _connect(self, host, port, secure):

        self.myprint(255, '_connect: %d %d %d %s %s' % (self.open, self.result, self.status, self.action, self.dstid))
        self._host = host
        self._port = port
        self.fdatapath = None
        self.bcount = 0L
        self.buffer = ''
        self.buffer_length = 0
        self.result = 0
        self.status = 0
        self.recvheader = {}
        self.response = None
        securetry = False
        if self.action not in MapAction.keys():
            raise Exception('no such action %s' % (self.action))
        if secure:
            securetry = True
            self.conn = httplib.HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout)
            try:
#                self.conn.request(self.action, self.url, headers=self.sendheader)
                if self.password:
                    self.sendheader['password']="%s" % (self.password)
                self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception, e:
                secure = False
                self.myprint(255, '_connect: error in secure request to %s:%d %s' % (host, port, e))
                self.conn.close()
                if self.sendheader.has_key('password'):
                    del self.sendheader['password']

        self.Secure = secure
        if not secure:
            self.conn = httplib.HTTPConnection(host, port, timeout=self.timeout)
            try:
#                self.conn.request(self.action, self.url, headers=self.sendheader)
                self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception, e:
                self.result = 1
                self.myprint(255, '_connect: error in request to %s:%d %s' % (host, port, e))
                self.conn.close()
        if not secure and self.result and not securetry:
            securetry = True
            self.conn = httplib.HTTPSConnection(host, port, cert_file=self.certfile, timeout=self.timeout)
            try:
                self.Secure = secure = True
#                self.conn.request(self.action, self.url, headers=self.sendheader)
                if self.password:
                    self.sendheader['password']="%s" % (self.password)
                self.conn.request(MapAction[self.action], self.url, headers=self.sendheader)
            except Exception, e:
                self.myprint(255, '_connect: error in secure request to %s:%d %s' % (host, port, e))
                self.conn.close()
                self.result = 1
                if self.sendheader.has_key('password'):
                    del self.sendheader['password']


        if not self.result:
            nextsendwas = self.nextsend
            self.myprint(255, '_connect: sendheader = %s, chksum = %s, nextsend = %s' % (self.sendheader, self.chksum, self.nextsend))
            if self.nextsend:
                self.nextsend = False
                rcount = 0
                try:
                    TS=time.time()
                    if self.chksum: self.md5sum = hashlib.md5()
                    data = self.f.read(self.write_buffer_size)
                    while data:
                        self.bcount += len(data)
                        if self.chksum: self.md5sum.update(data)
                        rcount += 1
                        while data:
                            sent = self.conn.sock.send(data)
                            self.myprint(255, '_connect: data sent = %d, spent %f' % (self.bcount, time.time()-TS))
                            data = data[sent:]
                        data = self.f.read(self.write_buffer_size)
                    self.f.close()
                    if self.chksum:
                        self.md5sum = self.md5sum.hexdigest()
                        self.chksum = False
                except Exception, e:
                    self.myprint(255, '_connect: rcount = %(rcount)d, exception = %(e)s' % vars())
                    self.result = 1
            try:
                self.response = self.conn.getresponse()
            except Exception, e:
                if self.response and hasattr(self.response, 'close'): self.response.close()
                if self.conn and hasattr(self.conn, 'close'): self.conn.close()
                self.myprint(0, '_connect: error in response from %s:%d (%s)' % (host, port, e))
                self.nextsend= nextsendwas
                self.result = 1

    def _respond(self):

        self.myprint(255, '_respond: %d %d %d %s %s' % (self.open, self.result, self.status, self.action, self.dstid))
        if self.result:
            if self.conn and hasattr(self.conn, 'close'): self.conn.close()
            return
        self.status = self.response.status
        self.recvheader = dict(self.response.getheaders())
        self.reason = self.response.reason
        self.myprint(255, '_respond: recvheader = %s' % self.recvheader)
        self.myprint(255, '_respond: status = %d' % self.status)
        self.myprint(255, '_respond: reason = %s' % self.reason)
        if 'content-type' in self.recvheader:
             self.ctype = self.recvheader['content-type']
        if 'content-length' in self.recvheader:
            self.content_length = long(self.recvheader['content-length'])
        if self.status == 200:
            self.result = 0
            if self.action in ["GETLOCALSTATS", "GETGROUPSTATS", "GETTOTALSTATS", "LOCATE", "LOCATEFILE", "LOCATELOCAL", "LOCATEREMOTE", "MD5SUM", "PING", "SIZE", "STAT"]:
                self.buffer_length = self.content_length
                self.buffer = self.response.read(self.buffer_length)
                while len(self.buffer) < self.content_length:
                     self.buffer += self.response.read(self.buffer_length - len(self.buffer))
                if self.response and hasattr(self.response, 'close'): self.response.close()
            elif self.action in ["STORED","DSSSTORED"]:
                pass
            elif self.action in ["STORE","DSSSTORE"]:
                self.nextsend = True
                if 'storecheck' in self.recvheader and 'storekey' in self.recvheader:
                    self.storecheck = self.recvheader['storecheck'] == 'OK'
                    self.storekey = self.recvheader['storekey']
                    self.chksum = True
                if 'JOBID' in self.recvheader:
                    self.jobid = self.recvheader['JOBID']
#                    self.md5sum = hashlib.md5()
#                data = self.f.read(self.write_buffer_size)
#                self.bcount += len(data)
#                while data:
#                    if data and self.chksum: self.chksum.update(data)
#                    while data:
#                        #sent = self.response.fp.write(data)
#                        sent = os.write(fp.fileno(), data)
#                        #breakpoint()
#                        #sent = self.conn.send(data)
#                        data = data[sent:]
#                    data = self.f.read(self.write_buffer_size)
#                    self.bcount += len(data)
#                self.f.close()
                self.response.fp.close()
            elif self.action in ["GET", "GETANY", "GETEXACT", "GETLOCAL", "GETREMOTE","DSSGET","DSSGETANY"]:
                if self.open == 3:
                    self.handle_handle.whead(self.ctype)
                    self.open = 1
                data = self.response.read(self.read_buffer_size)
                while not self.result and data:
                    if self.content_length:
                        if (self.bcount + len(data)) > self.content_length:
                            self.result = -2
                            break;
                    if self.open:
                        try:
                            self.f.write(data)
                        except Exception, e:
                            self.result = -1
                            self.open = 0
                            self.myprint(255, '_respond: Exception "%s:' % (e))
                            try:
                                    self.myprint(255, '_respond: '+traceback.format_exc())
                            except:
                                    self.myprint(255, '_respond: No traceback possible!')
                            print('_respond: Connection with client broken!')
                    if self.f_extra:
                        self.f_extra.write(data)
                    self.bcount += len(data)
                    self.myprint(128, '_respond: writing : %d' % len(data))
                    if self.buffer_length:
                        self.buffer = self.buffer + data
                    data = self.response.read(self.read_buffer_size)
                    if data != None:
                        self.myprint(128, '_respond: got %d bytes!' % (len(data)))
                    else:
                        self.myprint(128, '_respond: got None!')
                if self.response and hasattr(self.response, 'close'): self.response.close()
            elif self.action in ["HEAD", "HEADLOCAL"]:
                self.result = 0
                #self.response.close()
        elif self.status == 201:
            self.open = 2
        elif self.status == 204:
            self.result = 0
            if self.action in ["STORE","STORED","DSSSTORE","DSSSTORED"]:
                self.open = 2
                if 'datapath' in self.recvheader and 'storekey' in self.recvheader:
                    self.storecheck = True
                    self.storekey = self.recvheader['storekey']
                    datapath = os.sep.join([self.data_path, 'ydata', self.storekey+'_INCOMPLETE'])
                    #self.chksum = True
                    self.md5sum = hashlib.md5()
                    fdatapath = open(datapath, 'ab')
                    if not self.fdatapath:
                        self.result = -1
                    if not self.result:
                        data = self.f.read(self.write_buffer_size)
                        while data:
                            self.bcount += len(data)
                            self.md5sum.update(data)
                            fdatapath.write(data)
                            data = self.f.read(self.write_buffer_size)
                        fdatapath.close()
                    self.md5sum = self.md5sum.hexdigest()
                    #self.chksum = False
                    self.open = 1
            elif 'link-name' in self.recvheader:
                linkname = os.sep.join([self.data_path, 'xdata', self.recvheader['link-name']])
                localname = self.f.name
                self.f.close()
                self.f = None
                self.open = 0
                os.remove(localname)
                os.symlink(linkname, localname)
            elif self.action in ["DSSCACHEFILE"]:
                if 'JOBID' in self.recvheader:
                    self.jobid = self.recvheader['JOBID']
        elif self.status in [301, 302, 303, 307]:
            if 'location' in self.recvheader:
                if self.status in [301, 302, 307]:
                    self.result = 2
                elif self.status in [303]:
                    self.result = 3
                redirhost = urlparse.urlparse(self.recvheader['location'])
                self.myprint(255, '_respond: redirhost = %s status =  %d' % (redirhost, self.status))
                h=''
                p=''
                hp_r = string.split(redirhost[1], ':')
                if len(hp_r)==2:
                    h=hp_r[0]
                    p=hp_r[1]
                elif len(hp_r)==1:
                    h=hp_r[0]
                    p=80
                else:
                    h=hp_r[0]
                    p=80
                self.host2 = h
                if p == '':
                    self.port2 = 80
                else:
                    self.port2 = int(p)
                if redirhost[0] == 'https':
                    self.secure2 = True
                else:
                    self.secure2 = False
                if self.status == 301:
                    self.store_port = self.port2
                    self.store_host = self.host2
                if self.status == 303:
                    inext = ''
                    ouext = ''
                    self.cmd = None
                    for ext in Compressed_Exts:
                        if ext and redirhost[2].endswith(ext): inext = ext
                        if ext and self.getpath.endswith(ext): ouext = ext
                    self.getpath = urllib.url2pathname(redirhost[2])
                    if inext and not ouext:
                        self.cmd = Decompressors[inext]
                    elif not inext and ouext:
                        self.cmd = Compressors[ouext]
                    elif inext and ouext:
                        self.cmd = Decompressors[inext] + '| ' + Compressors[ouext]
                    self.myprint(255, '_respond: cmd = %s' % self.cmd)
            else:
                self.result = 1
            self.myprint(255, '_respond: result = %d status = %d' % (self.result, self.status))
        elif self.status == 400:
            self.result = 1
        elif self.status == 403:
            self.result = 1
        elif self.status == 404:
            self.result = 1
        elif self.status == 501:
            self.result = 1
        elif self.status == 503:
            self.result = 1
            self.myprint(0, '_respond: Server too busy to handle request! [dstid = %s]' % (self.dstid))
            self.response.fp.close()
            sleeptime = 60.0
            if 'sleep-time' in self.recvheader['sleep-time']:
                time.sleep(float(self.recvheader['sleep-time']))
            return
        else:
            self.result = 1
#        self.response.fp.close()
#        if self.response and hasattr(self.response, 'close'): self.response.close()
#        if self.conn and hasattr(self.conn, 'close'): self.conn.close()
        if self.open == 2 or self.nextsend:
            pass
        elif self.open:
            if not self.autoclose:
                pass
            elif not self.result:
                self.f.close()
            self.open = 0

    def _setup(self, action, path, host=None, port=None, secure=None, query=None, use_data_path=True, fileURI=None, jobid=None, username='', password='', **kw):
        '''
        sets up the connection string and connection
        '''
        if not host: host = self.host
        if not port: port = self.port
        self.Secure = None
        if not secure: secure = self.secure
        self.myprint(255, '_setup: host:port = %s:%d' % (host, port))
        self.action = action
        if action in ["STORE","STORED","DSSSTORE","DSSSTORED"]:
            head, path = os.path.split(path)
        if action != "DELETE" and os.path.isabs(path): path = path[len(os.path.sep):]
        if action.startswith("GET") or action.startswith("DSSGET"):
            self.getpath = path
        rest = ''
        self.chksum = False
        self.sendheader = {}
        if kw:
            for (k,v) in kw.items():
                self.sendheader[k] = '%s' % (v)
        self.sendheader['Author'] = 'K.G. Begeman'
        self.sendheader['Client'] = 'httplib'
        self.sendheader['Client-Version'] = '%s' % (self.version)
        self.sendheader['DSTID'] = '%s' % (self.dstid)
        if self.recvheader and self.recvheader.has_key('ProxyHost'):
            hp_r = string.split(self.recvheader['ProxyHost'], ':')
            if len(hp_r)==2:
                h=hp_r[0]
                p=hp_r[1]
            elif len(hp_r)==1:
                h=hp_r[0]
                p=80
            else:
                h=hp_r[0]
                p=80
            self.sendheader['Host'] = '%s:%d' % (h, p)
        else:
            if self.host and not self.host[0].isdigit():
                self.sendheader['Host'] = '%s:%d' % (self.host, port)
            else:
                self.sendheader['Host'] = '%s:%d' % (socket.getfqdn(host), port)
        self.sendheader['TimeStamp'] = '%s' % (time.strftime('%Y-%m-%dT%H:%M:%S'))
        self.sendheader['Pragma']='%s' % (self.action)
        self.sendheader['Connection'] = 'close'
        if query:
            rest = '?%s' % query
        if action in ["STORE","DSSSTORE"]:
             if self.nextsend:
                 self.chksum = True
                 self.sendheader['RECEIVE'] = 'OK'
             else:
                 self.sendheader['RECEIVE'] = 'NOT OK'
             self.sendheader['StoreCheck'] = 'OK'
        if action in ["STORE","DSSSTORE"] and self.open:
            if self.sendheader['RECEIVE']=='OK':
                self.sendheader['Content-Length'] = '%d' % (os.fstat(self.f.fileno())[6])
#                self.sendheader['Connection'] = 'close'
#            self.sendheader['content-length'] = '%d' % (os.fstat(self.f.fileno())[6])
#            self.sendheader['content-length'] = int(os.fstat(self.f.fileno())[6])
                self.content_length = os.fstat(self.f.fileno())[6]
            else:
                self.sendheader['Content-Length'] = '0'
        else:
            self.content_length = 0
        if self.data_path and use_data_path:
            self.sendheader['Data-Path'] = '%s' % (self.data_path)
        if action in  ["STORED","DSSSTORED"] and self.storekey:
            self.nextsend = False
            self.sendheader['StoreKey'] = '%s' % (self.storekey)
            #if self.md5sum:
            self.sendheader['MD5SUM'] = '%s' % (self.md5sum)
            self.sendheader['BCOUNT'] = '%s' % (self.bcount)
            self.sendheader['Content-Length'] = '0'
        if self.user_id:
            self.sendheader['User-ID'] = '%s' % (self.user_id)
        if self.machine_id:
            self.sendheader['Machine-ID'] = '%s' % (self.machine_id)
        if fileURI:
            self.sendheader['URI'] = '%s' % (fileURI)
        if action=="DSSGETLOG" and jobid:
            self.sendheader['JOBID'] = '%s' % (jobid)
        if username:
            self.sendheader['username'] = '%s' % (username)
        if password:
            self.password=password
            self.sendheader['password'] = '%s' % (password)

        self.url = '/%s%s' % (path, rest)
        hosts = socket.gethostbyname_ex(host)[2]
        self.myprint(255, '_setup: hosts = %s' % hosts)
        if len(hosts) > 1:
            hosts.sort()
            l = len(hosts)
            shift = random.randrange(0, l)
            hs = [hosts[(shift+i)%l] for i in range(l)]
            hosts = hs
        for h in hosts:
            self.status = -1
            while self.status == -1:
                self._connect(h, port, secure)
                if not self.result:
                    self._respond()
                else:
                    break
                #if self.status == 503: time.sleep(60.0)
            if not self.result: break

    def delete(self, path, defaultkey, host=None, port=None):
        '''
        delete removes a file from a server, i.e. it moves the file to the
        ddata directory on the servers disk. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        text = []
        r = gpgencrypt([path], defaultkey, fout=None, lout=text)
        if r: return False
        ptext = pickle.dumps(text)
        self._setup("DELETE", ptext, host, port)
        if self.result:
            return False
        else:
            return True

    def md5sum(self, path, host=None, port=None):
        '''
        md5sum calculates the md5 check sum a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("MD5SUM", path, host, port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
        return r

    def size(self, path, host=None, port=None):
        '''
        size calculates the size of a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("SIZE", path, host, port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = self.buffer[:self.buffer_length]
        else:
            r = ''
        return r

    def stat(self, path, host=None, port=None):
        '''
        stat returns the stat tuple a file. The file must exist on the
        contacted server, the path must be returned from a locate call.

        path    = name of the file as returned by locate.
        host    = optional host name where the file resides.
        port    = optional port number of the server where the file resides.
        '''

        self.open = 0
        self._setup("STAT", path, host, port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = pickle.loads(urllib.url2pathname(self.buffer[:self.buffer_length]))
        else:
            r = ()
        return r

    def getstats(self, mode='LOCAL'):

        self.open = 0
        path = 'dummy'
        if mode in ['GLOBAL', 'TOTAL']:
            self._setup("GETTOTALSTATS", path, self.host, self.port)
        elif mode == 'GROUP':
            self._setup("GETGROUPSTATS", path, self.host, self.port)
        else:
            self._setup("GETLOCALSTATS", path, self.host, self.port)
        if self.result:
            r = None
        elif self.buffer_length:
            r = string.split(self.buffer[:self.buffer_length], '<>')
        else:
            r = []
        return r

    def _locateit(self, path, local=False, remote=False):

        self.open = 0
        if local and remote:
            self._setup("LOCATE", path, self.host, self.port)
        elif not local and not remote:
            self._setup("LOCATEFILE", path, self.host, self.port)
        elif local and not remote:
            self._setup("LOCATELOCAL", path, self.host, self.port)
        elif not local and remote:
            self._setup("LOCATEREMOTE", path, self.host, self.port)
        if self.result:
            if self.status == 404:
                r = []
            else:
                r = None
        elif self.buffer_length:
            r = string.split(self.buffer[:self.buffer_length], '<>')
        else:
            r = []
        return r

    def locate(self, path):
        '''
        locate locates a file, i.e. returns the url of the server
        where the specified file resides. All data servers are probed
        for the file.

        path    = name of the file
        '''

        return self._locateit(path, local=True, remote=True)

    def locatefile(self, path):
        '''
        locatelocal locates a file, i.e. returns the url of the server
        where the specified file resides. Only the addressed dataserver
        is probed.

        path    = name of the file
        '''

        return self._locateit(path, local=False, remote=False)

    def locatelocal(self, path):
        '''
        locatelocal locates a file, i.e. returns the url of the server
        where the specified file resides. Only the local dataservers
        are probed.

        path    = name of the file
        '''

        return self._locateit(path, local=True, remote=False)

    def locateremote(self, path):
        '''
        locateremote locates a file, i.e. returns the url of the server
        where the specified file resides. Only remote dataservers are
        probed.

        path    = name of the file
        '''

        return self._locateit(path, local=False, remote=True)

    def testfile(self, path):
        '''
        testfile tests whether a file exists on the server

        path    = name of the file
        '''
        self.open = 0
        self._setup("TESTFILE", path, self.host, self.port)
        if self.result == 0 and self.status == 204:
            return True
        else:
            return False

    def testcache(self):
        '''
        testcache tests whether the server is allowed to cache files
        '''
        self.open = 0
        self._setup("TESTCACHE", 'testcache', self.host, self.port)
        if self.result == 0:
            return True
        else:
            return False

    def teststore(self):
        '''
        teststore tests whether the server is allowed to store files
        '''
        self.open = 0
        self._setup("TESTSTORE", 'teststore', self.host, self.port)
        if self.result == 0 and self.status == 204:
            return True
        else:
            return False

    def _getit(self, path=None, fd=None, local=False, remote=True, exact=False, handle=None, extra=None, raise_exception=False, query=None, savepath=None, username='', password=''):
        '''
        _getit handles the retrieving of files from local or remote servers

        path    = name of the file
        local   = local server (True) or only remote (False)
        remote  = remote server (True) or local server (False)
        handle  = handle to the DataRequestHandler
        extra   = extra fd to write to
        query   = extra url query
        savepath= name of file where to store the retrieved data [name of originalfile]
        exact   = exact location of file is given/wanted
        '''
        fileURI=None
        if path.find("://")>-1:
            fileURI=path
            path=path.split("/")[-1]
        if exact:
            action = "GETEXACT"
        elif local:
            if remote:
                if handle or fd:
                    action = "DSSGET"
                else:
                    action = "DSSGETANY"
            else:
                action = "GETLOCAL"
        else:
            action = "GETREMOTE"
        use_data_path = False
        if extra:
            self.f_extra = extra
        if handle:
            self.autoclose = False
            self.f = handle.wfile
            self.handle_handle = handle
            self.open = 3
            self._setup(action, path, self.host, self.port, query=query,username=username, password=password)
            self.myprint(255, '_getit: result = %d' % self.result)
            while self.result > 1 and not exact:
                self.open = 3
                self._setup(action, path, self.host2, self.port2, self.secure2, query=query,username=username, password=password)
                self.myprint(255, '_getit: result = %d' % self.result)
            self.handle_handle = None
            self.open = 0
            lpath = None
        else:
            if fd:
                self.autoclose = False
                if hasattr(fd, 'name'):
                    lpath = fd.name
                else:
                    lpath = None
                self.f = fd
            else:
                self.autoclose = True
                if savepath:
                    lpath = savepath
                else:
                    head, tail = os.path.split(path)
                    if tail == '':
                        lpath = head
                    else:
                        lpath = tail
                if not os.path.exists(lpath):
                    self.f = open(lpath + '_INCOMPLETE', "wb")
                    self.myprint(255, '_getit: OPEN ' + lpath + '_INCOMPLETE')
                    if not extra: use_data_path = True
                else:
                    self.f = None
            if self.f:
                self.open = 1
                self._setup(action, path, self.host, self.port, query=query, use_data_path=use_data_path, username=username, password=password)
                self.myprint(255, '_getit: _setup result = %d' % self.result)
                if self.result == 0 and self.status == 204 and use_data_path:
                    self.autoclose = True
                self.myprint(255, '_getit: result = %d' % self.result)
                while self.result > 1 and not exact:
                    if self.open:
                        self.f.seek(0)
                        self.f.truncate()
                    elif self.autoclose and lpath:
                        if self.result == 3:
                            self.f = os.popen(self.cmd + ' > ' + lpath + '_INCOMPLETE', "wb")
                            self.myprint(255, '_getit: POPEN %s > %s' % (self.cmd, lpath + '_INCOMPLETE'))
                        else:
                            self.f = open(lpath + '_INCOMPLETE', "wb")
                    self.open = 1
                    if self.result == 3:
                        self._setup("DSSGET", self.getpath, self.host2, self.port2, self.secure2, query=query, username=username, password=password)
                    else:
                        self._setup(action, path, self.host2, self.port2, query=query, username=username, password=password)
                    self.myprint(255, '_getit: result = %d' % self.result)
            else:
                self.open = 0
                self.result = 0
            if self.result:
                self._clear(lpath)
                if raise_exception:
                    raise IOError("Error retrieving remote file %s from %s:%d [DSTID=%s]" % (path, self._host, self._port, self.dstid))
        if self.open:
            self.f.close()
            self.open = 0
        if self.result:
            if self.autoclose and lpath and os.path.exists(lpath + '_INCOMPLETE'):
                os.remove(lpath + '_INCOMPLETE')
            self.autoclose = True
            return False
        else:
            if self.autoclose and lpath and os.path.exists(lpath + '_INCOMPLETE'):
                os.rename(lpath + '_INCOMPLETE', lpath)
            self.autoclose = True
            return True

    def head(self, path=None, query=None):
        self._setup("HEAD", path, self.host, self.port, query=query)
        if self.result == 2:
            self._setup("HEAD", path, self.host2, self.port2, self.secure, query=query)
        if self.result == 0:
            return self.recvheader
        else:
            return None

    def headlocal(self, path=None, query=None):
        self._setup("HEADLOCAL", path, self.host, self.port, query=query)
        if self.result == 0:
            return self.recvheader
        else:
            return None

    def get(self, path=None, fd=None, handle=None, extra=None, raise_exception=True, query=None, savepath=None, username='', password=''):
        '''
        get obtains the file from a remote server (see _getit)
        '''
        return self._getit(path=path, fd=fd, local=True, remote=True, exact=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath, username=username, password=password)

    def getlocal(self, path=None, fd=None, handle=None, extra=None, raise_exception=False, query=None, savepath=None, username='', password=''):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        return self._getit(path=path, fd=fd, local=True, remote=False, exact=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath, username=username, password=password)

    def getremote(self, path=None, fd=None, handle=None, extra=None, raise_exception=False, query=None, savepath=None, username='', password=''):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        return self._getit(path=path, fd=fd, local=False, remote=False, exact=False, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath, username=username, password=password)

    def getexact(self, path=None, fd=None, handle=None, extra=None, raise_exception=False, query=None, savepath=None, username='', password=''):
        '''
        getlocal obtains the file from a local server (see _getit)
        '''
        return self._getit(path=path, fd=fd, local=False, remote=False, exact=True, handle=handle, extra=extra, raise_exception=raise_exception, query=query, savepath=savepath, username=username, password=password)

    def register(self, path, validation):
        '''
        register gets a dataserver to register a file which is already
        in one of the dataserver directories.

        path    = path to the file (w.r.t. the working directory)
        '''
        self.open = 0
        self._setup("REGISTER", path, self.host, self.port, validation=validation)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def release(self, validation):
        '''
        release gets a dataserver to release the port the server is listening on
        '''
        self.open = 0
        self._setup("RELEASE", '/dummy', self.host, self.port, validation=validation)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def takeover(self, validation, newport, certfile=''):
        '''
        takeover gets a dataserver to take over the port the current server is listening on
        '''
        if not certfile: certfile = self.certfile
        self.open = 0
        query = ''
        if certfile:
            query = urllib.urlencode({'NEWPORT': newport, 'CERTFILE': certfile})
        else:
            query = urllib.urlencode({'NEWPPORT': newport})
        self._setup("TAKEOVER", '/dummy', self.host, self.port, validation=validation, newport=newport, certfile=certfile)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def reload(self, validation, force=False):
        '''
        reload lets a dataserver rescan the directory structure


        force   = True forces, False doesn't
        '''
        self.open = 0
        self._setup("RELOAD", '/dummy', self.host, self.port, force=force, validation=validation)
        if self.result == 0 and self.status == 200:
            return True
        else:
            return False

    def cachefile(self, path, savepath=None, username='', password='', fd=None):
        '''
        cachefile requests a caching server to cache a file

        path    = name of the file
        '''
        fileURI=None
        if path.find("://")>-1:
            fileURI=path
            path=path.split("/")[-1]

        self.open = 0
        self._setup("DSSCACHEFILE", path, self.host, self.port,fileURI=fileURI, username=username, password=password)
        if self.result:
            raise IOError("Error storing file %s on %s:%d [DSTID=%s]" % (path, self._host, self._port, self.dstid))
        if self.result == 0 and self.status == 204:
            if 'JOBMSG' in self.recvheader:
                jobmsg=self.recvheader['JOBMSG']
                if jobmsg.find('FINISHED')>-1:
                    return True
                elif jobmsg.find('FAILED')>-1:
                    return False
                else:
                    pass
            return True
        else:
            return False

    def put(self, path, savepath=None, username='', password='', fd=None):
        '''
        put stores a file on a local dataserver

        path    = path to local file
        '''
        fileURI=None
        if path.find("://")>-1:
            fileURI=path
            path=path.split("/")[-1]
        if savepath is None or len(savepath)==0:
            savepath=path
        if not os.path.exists(savepath):
            raise IOError("Error storing non-existent local file: %s" % (path))
        else:
            self.f = open(savepath, "rb")
            self.open = 1
            self._setup("DSSSTORE", path, self.store_host, self.store_port, use_data_path=self.data_path, username=username, password=password)
            if self.result == 2:
                self.store_host = self.host2
                self.store_port = self.port2
                self.store_secure = self.secure2
                if not self.open:
                    self.f = open(savepath, "rb")
                    self.open = 1
                self._setup("DSSSTORE", path, self.store_host, self.store_port, self.store_secure, use_data_path=self.data_path, username=username, password=password)
            if not self.result and self.nextsend:
                self._setup("DSSSTORE", path, self.store_host, self.store_port, self.store_secure, use_data_path=False, username=username, password=password)
            if not self.result and self.storecheck: #(self.status == 204 or self.storecheck):
                self._setup("DSSSTORED", path, self._host, self._port, username=username, password=password)
                if self.result:
                    Message('put: error executing DSSSTORED [result=%d]' % (self.result))
            if self.result:
                raise IOError("Error storing file %s on %s:%d [DSTID=%s]" % (path, self._host, self._port, self.dstid))
        if self.result:
            return False
        else:
            if 'JOBID' in self.recvheader:
                while True:
                    time.sleep(5)
                    self._setup("DSSGETLOG", path, self._host, self._port, jobid=self.jobid, username=username, password=password)
                    if self.result==0:
                        if 'JOBMSG' in self.recvheader:
                            jobmsg=self.recvheader['JOBMSG']
                            if jobmsg.find('FINISHED')>-1:
                                return True
                            elif jobmsg.find('FAILED')>-1:
                                return False
                            else:
                                pass
                        else:
                            return False
            return True

    def mirrorput(self, path, port=8000, directory=''):
        '''
        mirrorput make the data server retrieve file path to its sdata directory

        path        = path to local file
        port        = port local ds listens on
        directory   = change the directory where the file should be mirrored
        '''
        fs = os.stat(path)
        query = urllib.urlencode({'INFO': pickle.dumps((fs[stat.ST_ATIME], fs[stat.ST_MTIME], port, directory))})
        self._setup("MIRRORSTORE", path, self.host, self.port, query=query)
        if self.result:
            self.buffer_length = 0
            return False
        else:
            self.buffer_length = 0
            return True

    def getcaps(self):
        '''
        Get the capability string from server.
        '''
        if self.server_id == '' and self.server_caps == '':
            self.ping()
        return self.server_caps

    def getid(self):
        '''
        Get the server id.
        '''
        if self.server_id == '' and self.server_caps == '':
            self.ping()
        return self.server_id

    def ping(self):
        '''
        See if there is a server running out there and get the server info.
        '''
        curtime = time.time()
        self._setup("PING", 'ping', self.host, self.port)
        if not self.result:
            self.server_ping_time = time.time() - curtime
        if self.result:
            self.server_id = ''
            self.server_caps = ''
            self.server_ping_time = 0.0
            self.buffer_length = 0
            return False
        else:
            self.server_id = ''
            self.server_caps = ''
            self.buffer_length = 0
            split_buffer = string.split(self.buffer)
            if len(split_buffer) >= 1:
                self.server_id = split_buffer[0]
            if len(split_buffer) >= 2:
                self.server_caps = split_buffer[1]
            if len(split_buffer) < 1:
                self.result = 1
                return False
            else:
                return True

    def _ping(self, host, port):
        '''
        See if there is a server running out there
        '''
        r = True
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.connect((host, port))
        except Exception, e:
            r = False
        s.close()
        return r

    def _clear(self, path):
        '''
        Clean up after problems with retrieving.
        '''
        if self.open:
            self.f.close()
        self.open = 0
        if os.path.exists(path):
            os.remove(path)
        return self.result

    def checksum(self, path):
        '''
        checksum locates a file and returns for each result the path, host, port and checksum
        '''
        r = []
        ir = self.locate(path)
        for i in ir:
            d = dataserver_result_to_dict(i)
            if d['path'] != '?': r.append((d['ip'], d['port'], d['path'], self.md5sum(d['path'], d['ip'], int(d['port']))))
        return r

class Storage(Data_IO):

    def retrieve(self, data_object):
        Message('Retrieving %s' % data_object.pathname)
        begin = time.time()
        if hasattr(data_object, 'subimage'):
            x1, y1, x2, y2 = data_object.subimage
            fhi = str(x2)+','+str(y2)
            flo = str(x1)+','+str(y1)
            query = urllib.urlencode({'FHI':fhi, 'FLO':flo})
            #query = 'RANGE2=%(x2)d,%(y2)d&RANGE1=%(x1)d,%(y1)d' % vars()
            filename = os.sep.join([data_object.filepath, data_object.filename])
            fetch_file_from_dataserver(filename, query=query, savepath=data_object.pathname)
        else:
            self._set_data_path()
            self.get(path=data_object.pathname)
        end = time.time()
        total = end-begin
        if data_object.exists():
            if total >= 0.005:
                size = os.path.getsize(data_object.pathname)/2**10
                Message('Retrieved %s[%dkB] in %.2f seconds (%.2fkBps)' % (data_object.pathname, size, total, size/total))
            else:
                Message('Retrieved %s' % data_object.pathname)

    def store(self, data_object):
        Message('Storing %s' % data_object.pathname)
        begin = time.time()
        self._set_data_path()
        self.put(path=data_object.pathname)
        data_object.set_stored()
        end = time.time()
        total = end-begin
        if data_object.exists():
            if total >= 0.005:
                size = os.path.getsize(data_object.pathname)/2**10
                Message('Stored %s[%dkB] in %.2f seconds (%.2fkBps)' % (data_object.pathname, size, total, size/total))
            else:
                Message('Stored %s' % data_object.pathname)

    def object_to_commit(self, data_object):
        return data_object

def fetch_file_from_dataserver(filename, hostport=None, savepath=None, query=None):
    '''
    method to retrieve a file from a dataserver
    '''
    if savepath and os.path.exists(savepath):
        raise Exception, 'File  %s already exists!' % savepath
    if not savepath and os.path.exists(filename):
        raise Exception, 'File  %s already exists!' % filename
    if not hostport:
        hostport = Env['data_server'] + ':' + Env['data_port']
    c = Data_IO(hostport)
    return c.get(filename, query=query, savepath=savepath)

def messageIAL(command,filename,localfilename,exitcode,errormessage):
    m_type_1='''filename=%s\nfileuri=%s\nexitcode=%s\nmessage=%s\n'''
    m_type_2='''filename=%s\nstatus=%s\nmessage=%s\n'''
    message=''
    if command=='store' or command=='retrieve':
       if exitcode:
           message = m_type_1 % (filename, localfilename,0,'')
       else:
           message = m_type_1 % (filename, localfilename, 1, errormessage)
    elif command=='make_local':
       if exitcode:
           message = m_type_2 % (filename,'COMPLETED','')
       else:
           message = m_type_2 % (filename,'ERROR', errormessage)
    elif command=='ping':
        message="exitcode=%s\nmessage=%s"%(exitcode,errormessage)
    else:
       message = m_type_1 % ('','',1,'Unknown command')
    Message(message)
    return

def get_params(args):
    path=args[3]
    localpath=None
    debug=0
    if len(args)==4:
        localpath=args[3]
    elif len(args)==5:
        localpath=args[3]
        try:
            debug = int(args[4])
        except:
            debug=0
            localpath=args[4]
    elif len(args)==6:
        localpath=args[4]
        debug = int(args[5])
    else:
        usage()
        sys.exit(0)
    return path, localpath, debug


def main():
    table = {
        'cachefile':    'cachefile',
        'get':          'get',
        'getcaps':      'getcaps',
        'getexact':     'getexact',
        'getid':        'getid',
        'getlocal':     'getlocal',
        'getremote':    'getremote',
        'getstats':     'getstats',
        'delete':       'delete',
        'head':		'head',
        'headlocal':	'headlocal',
        'locate':       'locate',
        'locatefile':   'locatefile',
        'locatelocal':  'locatelocal',
        'locateremote': 'locateremote',
        'md5sum':       'md5sum',
        'mirrorput':    'mirrorput',
        'ping':         'ping',
        'register':     'register',
        'release':      'release',
        'reload':       'reload',
        'retrieve':     'get',
        'size':         'size',
        'stat':         'stat',
        'store':        'put',
        'testfile':     'testfile',
        'testcache':    'testcache',
        'teststore':    'teststore',
    }

    table_dss = {
        'retrieve':'get',
        'store':'put',
        'make_local':'cachefile',
        'ping':'ping',
    }

    def usage():
        Message('''\
Usage:  %s operation=<o> server=<s> filename=<f> certfile=<c> debug=<d> secure=<b> query=<q>

        <o>     wanted operation, one of %s
        <s>     wanted server(s) in hostname:portnumber format
        <f>     filename
        <c>     certfile with certificate and key [no secure connect]
        <d>     debuglevel, 0 to 255 [0]
        <b>     True or False (don't need a certfile) [False]
        <q>     query to be attached to the filename

''' % (os.path.basename(sys.argv[0]), string.join([k for k in table.keys()],',')))
        sys.exit(0)

    def usage_dss():
        Message('''\
Usage:  %s operation=<o> server=<s> filename=<f> [localfilename=<l>] certfile=<c> debug=<d> secure=<b> query=<q> username=<u> password=<p>

        <o>     wanted operation, one of %s
        <s>     wanted server(s) in [http://|https://]hostname:portnumber format
        <f>     filename
        <l>     local filename to be used instead of filename, optional
        <c>     certfile with certificate and key [no secure connect]
        <d>     debuglevel, 0 to 255 [0]
        <b>     True or False (don't need a certfile) [False]
        <q>     query to be attached to the filename
        <u>     username, username=ticket for SSO ticket
        <p>     password or SSO ticket

''' % (os.path.basename(sys.argv[0]), string.join([k for k in table_dss.keys()],', ')))
        sys.exit(0)



    uvars = {
        'operation':    '',
        'server':       '',
        'filename':     '',
        'localfilename':'',
        'certfile':     '',
        'debug':        0,
        'secure':       True,
        'query':        '',
        'fd':           '',
        'username':     '',
        'password':     '',
    }
    if len(sys.argv) < 3 or len(sys.argv) > 9: usage_dss()
    for arg in sys.argv[1:]:
        (key, val) = string.split(arg, '=', maxsplit=1)
        if key in uvars.keys():
            if type(uvars[key]) == str:
                uvars[key] = val
            else:
                uvars[key] = eval(val)
        else:
            raise Exception, 'Unknown key "%(key)s"' % vars()
    if uvars['operation'] not in table_dss.keys(): raise Exception, 'Unknown operation "%s"' % uvars['operation']
#    if table[uvars['operation']] not in ['getcaps', 'getid', 'getstats', 'ping', 'release', 'reload', 'testcache', 'teststore'] and not uvars['filename']: raise Exception, 'Need filename'
    if table_dss[uvars['operation']] not in ['ping'] and not uvars['filename']: raise Exception, 'Need filename'
    if uvars['server'].find("://") >-1:
        (protocol,server)=uvars['server'].split("://")
        if protocol=='https':
            uvars['secure']=True
        else:
            uvars['secure']=False
        uvars['server']=server
    ds_connect = Data_IO(uvars['server'], debug=uvars['debug'], secure=uvars['secure'], certfile=uvars['certfile'])
    errormes=''
    try:
        if uvars['filename']:
            fd = None
            if uvars['fd']:
                fd = open(uvars['fd'], 'w')
            if uvars['query']:
                result = getattr(ds_connect, table_dss[uvars['operation']])(uvars['filename'], savepath=uvars['localfilename'], query=uvars['query'], username=uvars['username'], password=uvars['password'], fd=fd)
            else:
                result = getattr(ds_connect, table_dss[uvars['operation']])(uvars['filename'], savepath=uvars['localfilename'], username=uvars['username'], password=uvars['password'],fd=fd)
        else:
            result = getattr(ds_connect, table[uvars['operation']])()
    except Exception, errmes:
        errormes=str(errmes)
        if ds_connect.response:
            errormes+=' DSS Server message:'
            errormes+=str(ds_connect.response.reason)
        result = None
        traceback.print_tb(sys.exc_info()[2])
    if errormes:
#        Message("operation %s failure!  Reason: %s" %(uvars['operation'], errmes))
        messageIAL(uvars['operation'], uvars['filename'], uvars['localfilename'], False, errormes)
    else:
#        Message("operation %s success!  Result: %s" %(uvars['operation'], result))
        messageIAL(uvars['operation'], uvars['filename'], uvars['localfilename'], True, errormes)

if __name__ == '__main__':
    main()
