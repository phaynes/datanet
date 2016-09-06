
# FIREWALL TCP INBOUND/OUTBOUND (Discovery-port, 2*Central-ports, 1*Agent-port)

INSTALL_DIR=${1:-"/vagrant"}
INSTALL_IP="127.0.0.1"

cd $INSTALL_DIR
mkdir -p node_modules

# NODE REPO
curl -sL https://deb.nodesource.com/setup | sudo bash -

# MONGO REPO
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 7F0CEB10
echo 'deb http://downloads-distro.mongodb.org/repo/ubuntu-upstart dist 10gen' | sudo tee /etc/apt/sources.list.d/mongodb.list
sudo apt-get update

sudo apt-get install -y build-essential mongodb-org
sudo cp /vagrant/init/mongodb.service /etc/systemd/system/
sudo systemctl enable /etc/systemd/system/mongodb.service
sudo systemctl start mongodb.service

# ADDITIONAL REPOS
sudo apt-get install -y nodejs haproxy git ntp

# PRINT OUT VERSIONS
node --version
echo "db.version()" | mongo

# INSTALL NODE MODULES
npm install keypress
npm install ws
#  Need to use 1.4 as the plugin code uses old connection semantics.
#  Move code to use newer 2.0 mongodb client connection pieces.
npm install mongodb@1.4 
npm install bcrypt

(cd node_modules; ln -s /vagrant zync)

# BUILD zync_all.js for ZyncClient.html
(cd node_modules/zync/;
  (cd static/; ./create_zync_all_js.sh)
)

# /etc/hosts ENTRIES
for dc in USA JAP IRE; do
  if ! grep "$dc" /etc/hosts; then
    echo "$INSTALL_IP	$dc" | sudo tee -a /etc/hosts
  fi
done

# (cd node_modules/zync/;
#   export ZYNC_SRC_PATH=${INSTALL_DIR}
#   echo ./bin/one_script_start_demo.sh ">" /tmp/LOG_DEMO.log
#   ./bin/one_script_start_demo.sh > /tmp/LOG_DEMO.log 2>&1
# )

echo "TO START DEMO"
echo "1.) cd node_modules/zync/"
echo "2.) . ./debug_aliases.sh"
echo "3.) . ./demos/heartbeat/scripts/initialize_six_agents.sh"
echo "4.) init_six_agents_for_demo"
echo "5.) MANUALLY OK SSL certificates for https://usa:10100/GlobalKeyMonitor.html https://jap:10200/GlobalKeyMonitor.html https://ire:10300/GlobalKeyMonitor.html"

