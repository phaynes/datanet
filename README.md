
# SUMMARY
Datanet is an open source CRDT based data synchronization system. Datanet is a P2P replication system that utilizes CRDT algorithms to allow multiple concurrent actors to modify data and then automatically & sensibly resolve modification conflicts. Datanet's goal is aims to achieve ubiquitous write though caching. CRDT replication capabilities can be added to any cache in your stack, meaning modifications to these stacks are globally & reliably replicated. Locally modifying data yields massive gains in latency, produces a more efficient replication stream, & is extremely robust. It’s time to pre-fetch data to compute :)

# LINKS
  Website: (http://datanet.co/)

  Community: (https://groups.google.com/forum/#!forum/crdtdatanet)

  Twitter: (https://twitter.com/CRDTDatanet)

# FEDORA INSTALL
```
  tar xvfz datanet_0.X.X.tgz
  cd datanet_0.X.X
  ./FEDORA_INSTALL.sh
```

# UBUNTU INSTALL
```
  tar xvfz datanet_0.X.X.tgz
  cd datanet_0.X.X
  ./UBUNTU_INSTALL.sh
```
# QUICK START
## START CENTRAL:
```
(cd datanet_central_0.X.X
  export ZYNC_SRC_PATH=${PWD}
  . ./debug_aliases.sh
  (cd init; ./init_demo_db.sh)
  DEMO_DC1-1-BOTH
)
```

## START NODE.JS AGENT:
```
(cd datanet_central_0.X.X
  export ZYNC_SRC_PATH=${PWD}
  . ./debug_aliases.sh
  DEMO_DC1-1-SINGLE-AGENT
)
```

## RUN DATANET_OPENRESTY_AGENT:
```
export PATH="$PATH:/usr/local/openresty/nginx/sbin/"
(cd /usr/local/datanet;
  nginx -c /usr/local/datanet/conf/minimal_nginx.conf -p /usr/local/datanet
)
```

# DOWNLOADS
http://datanet.co/download.html

# DOCUMENTATION
http://datanet.co/documentation.html

# PRESENTATIONS
http://datanet.co/resources.html

