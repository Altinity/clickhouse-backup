# golang
apt-get update && apt-get install -y software-properties-common
apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 52B59B1571A79DBC054901C0F6BC817356A3D45E
add-apt-repository -y ppa:longsleep/golang-backports
apt-get update
apt-get purge -y golang*
apt-get install -y golang-1.18

mkdir -p ~/go/
export GOPATH=~/go/
grep -q -F 'export GOPATH=$GOPATH' ~/.bashrc  || echo "export GOPATH=$GOPATH" >> ~/.bashrc
grep -q -F 'export GOPATH=$GOPATH' /root/.bashrc         || echo "export GOPATH=$GOPATH" >> /root/.bashrc
export GOROOT=/usr/lib/go-1.18/
grep -q -F 'export GOROOT=$GOROOT' ~/.bashrc  || echo "export GOROOT=$GOROOT" >> ~/.bashrc
grep -q -F 'export GOROOT=$GOROOT' /root/.bashrc || echo "export GOROOT=$GOROOT" >> /root/.bashrc
ln -nsfv /usr/lib/go-1.18/bin/go /usr/bin/go

CGO_ENABLED=0 GO111MODULE=on go install -ldflags "-s -w -extldflags '-static'" github.com/go-delve/delve/cmd/dlv@latest

# GO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags osusergo,netgo -gcflags "all=-N -l" -ldflags "-extldflags '-static' -X 'main.version=debug'" -o build/linux/amd64/clickhouse-backup ./cmd/clickhouse-backup
# ~/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /usr/bin/clickhouse-backup download increment_59690570474117865

/root/go/bin/dlv --listen=:40001 --headless=true --api-version=2 --accept-multiclient exec /bin/clickhouse-backup -- -c /etc/clickhouse-server/config.d/ch-backup.yaml upload debug_upload --table

