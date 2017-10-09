from thrift.transport.TTransport import *
from thrift.transport.TSocket import *
from thrift.Thrift import TException
from random import *
import os
import errno
import socket
import sys
import time



class TSocketPool(TSocket):

    serverStates = {}
    
    # TSocketPool([('192.168.10.85', 8090), ('192.168.10.87', 9090)])
    # or TSocketPool('192.168.10.86', 8754)

    def __init__(self, host, port=9090):
        TSocket.__init__(self)
        self.servers = []
        self.randomize = True
        self.retryInterval = 60
        self.numRetries = 1
        self.maxConsecutiveFailures = 1
        self.alwaysTryLast = True
        if type(port) is list:
            for i in range(0,len(port)):
                self.servers.append((host[i],int(port[i])))
        else:
            if type(host) is list:
                self.servers = host
            else:
                self.servers = [(host, int(port),)]

    def open(self):
        # Check if we want order randomization
        servers = self.servers
        if self.randomize:
            servers = []
            oldServers = []
            oldServers.extend(self.servers)
            while len(oldServers):
                posn = int(random()*len(oldServers))
                servers.append(oldServers[posn])
                oldServers[posn] = oldServers[-1]
                oldServers.pop()

        # Count servers to identify the "last" one
        for i in range(0, len(servers)):
            # This extracts the $host and $port variables 
            host, port = servers[i]
            # Check APC cache for a record of this server being down
            failtimeKey = 'thrift_failtime:%s%d~' % (host, port)
            # Cache miss? Assume it's OK
            lastFailtime = TSocketPool.serverStates.get(failtimeKey, 0)
            retryIntervalPassed = False
            # Cache hit...make sure enough the retry interval has elapsed
            if lastFailtime > 0:
                elapsed = int(time.time()) - lastFailtime
                if elapsed > self.retryInterval:
                    retryIntervalPassed = True

            # Only connect if not in the middle of a fail interval, OR if this      
            # is the LAST server we are trying, just hammer away on it      
            isLastServer = self.alwaysTryLast and i == (len(servers) - 1) or False      

            if lastFailtime == 0 or isLastServer or (lastFailtime > 0 and retryIntervalPassed):
                # Set underlying TSocket params to this one        
                self.host = host
                self.port = port
                # Try up to numRetries_ connections per server        
                for attempt in range(0, self.numRetries):
                    try:
                        # Use the underlying TSocket open function            
                        TSocket.open(self)
                        # Only clear the failure counts if required to do so            
                        if lastFailtime > 0:
                            TSocketPool.serverStates[failtimeKey] = 0
                        # Successful connection, return now            
                        return          
                    except TTransportException, e:            
                        # Connection failed          
                        pass

                # Mark failure of this host in the cache        
                consecfailsKey = 'thrift_consecfails:%s%d~' % (host, port)
                # Ignore cache misses        
                consecfails = TSocketPool.serverStates.get(consecfailsKey, 0) 

                # Increment by one        
                consecfails += 1         
                # Log and cache this failure        
                if consecfails >= self.maxConsecutiveFailures:
                    # Store the failure time          
                    TSocketPool.serverStates[failtimeKey] =  int(time.time())
                    # Clear the count of consecutive failures          
                    TSocketPool.serverStates[consecfailsKey] = 0       
                else:
                    TSocketPool.serverStates[consecfailsKey] = consecfails

        # Oh no; we failed them all. The system is totally ill!    
        error = 'TSocketPool: All hosts in pool are down. ';    
        hostlist = ','.join(['%s:%d' % (s[0], s[1]) for s in self.servers])
        error += '(%s)' % hostlist

        raise TException(error)

