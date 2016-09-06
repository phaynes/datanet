
# FIREWALL TCP INBOUND/OUTBOUND (Discovery-port, 2*Central-ports, 1*Agent-port)

INSTALL_DIR="/home/user"
INSTALL_IP="127.0.0.1"

cd $INSTALL_DIR

# NODE REPO
curl -sL https://deb.nodesource.com/setup | sudo bash -

# MONGO REPO
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update

# ADDITIONAL REPOS
sudo apt-get -y install build-essential nodejs mongodb-org haproxy git ntp

sudo start mongod

# PRINT OUT VERSIONS
node --version
echo "db.version()" | mongo

# INSTALL NODE MODULES
npm install keypress
npm install ws
npm install mongodb
npm install bcrypt
npm install redis
npm install memcached
npm install msgpack-js
npm install lz4

(cd node_modules; git clone git@github.com:JakSprats/zync.git)

# BUILD zync_all.js for ZyncClient.html
(cd node_modules/zync/;
  (cd static/; ./create_zync_all_js.sh)
)

# /etc/hosts ENTRIES
grep USA /etc/hosts
RET=$?
if [ $RET -eq 1 ]; then
  echo "$INSTALL_IP	USA" >> /etc/hosts
fi
grep JAP /etc/hosts
RET=$?
if [ $RET -eq 1 ]; then
  echo "$INSTALL_IP	JAP" >> /etc/hosts
fi
grep IRE /etc/hosts
RET=$?
if [ $RET -eq 1 ]; then
  echo "$INSTALL_IP	IRE" >> /etc/hosts
fi

(cd node_modules/zync/;
  echo ./bin/one_script_start_demo.sh ">" /tmp/LOG_DEMO.log
  ./bin/one_script_start_demo.sh > /tmp/LOG_DEMO.log 2>&1
)

echo "TO START DEMO"
echo "1.) cd node_modules/zync/"
echo "2.) . ./debug_aliases.sh"
echo "3.) . ./demos/heartbeat/scripts/initialize_six_agents.sh"
echo "4.) init_six_agents_for_demo"
echo "5.) MANUALLY OK SSL certificates for https://usa:10100/GlobalKeyMonitor.html https://jap:10200/GlobalKeyMonitor.html https://ire:10300/GlobalKeyMonitor.html"

