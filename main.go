package main

import (
	"encoding/json"
	"fmt"
	"github.com/hashicorp/raft"
	"github.com/hashicorp/raft-boltdb"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"sync"
	"time"
)

type db struct {
	db *sync.Map
}

type payload struct {
	Key   string
	Value string
}

func (kv *db) Apply(log *raft.Log) any {
	switch log.Type {
	case raft.LogCommand:
		var sp payload
		err := json.Unmarshal(log.Data, &sp)
		if err != nil {
			return fmt.Errorf("could not parse payload: %s", err)
		}

		kv.db.Store(sp.Key, sp.Value)
	default:
		return fmt.Errorf("unknown raft log type: %#v", log.Type)
	}

	return nil
}

type snapshotNoop struct{}

func (sn snapshotNoop) Persist(_ raft.SnapshotSink) error { return nil }
func (sn snapshotNoop) Release() {}

func (kv *db) Snapshot() (raft.FSMSnapshot, error) {
	return snapshotNoop{}, nil
}

func (kv *db) Restore(rc io.ReadCloser) error {

	decoder := json.NewDecoder(rc)

	for decoder.More() {
		var sp payload
		err := decoder.Decode(&sp)
		if err != nil {
			return fmt.Errorf("could not decode payload %s", err)
		}
		kv.db.Store(sp.Key, sp.Value)
	}

	return rc.Close()
}

func setUpRaft(dir, nodeId, raftAddress string, kv *db) (*raft.Raft, error) {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return nil, fmt.Errorf("could not create data directory: %s", err)
	}

	store, err := raftboltdb.NewBoltStore(path.Join(dir, "bolt"))

	if err != nil {
		return nil, fmt.Errorf("could not create bolt store: %s", err)
	}

	snapshots, err := raft.NewFileSnapshotStore(path.Join(dir, "snapshot"), 2, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("could not create snapshot store: %s", err)
	}

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddress)
	if err != nil {
		return nil, fmt.Errorf("Could not resolve address: %s", err)
	}

	transport, err := raft.NewTCPTransport(raftAddress, tcpAddr, 10, time.Second*10, os.Stderr)
	if err != nil {
		return nil, fmt.Errorf("Could not create tcp transport: %s", err)
	}
	raftCfg := raft.DefaultConfig()
	raftCfg.LocalID = raft.ServerID(nodeId)

	r, err := raft.NewRaft(raftCfg, kv, store, store, snapshots, transport)
	if err != nil {
		return nil, fmt.Errorf("Could not create raft instance: %s", err)
	}

	// Cluster consists of unjoined leaders. Picking a leader and
	// creating a real cluster is done manually after startup.
	r.BootstrapCluster(raft.Configuration{
		Servers: []raft.Server{
			{
				ID:      raft.ServerID(nodeId),
				Address: transport.LocalAddr(),
			},
		},
	})

	return r, nil
}

type httpServer struct {
	r  *raft.Raft
	db *sync.Map
}

func (rs httpServer) setHandler(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	bs, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return

	}

	future := rs.r.Apply(bs, 500*time.Millisecond)
	if err := future.Error(); err != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	e := future.Response()
	if e != nil {
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusOK)
}

func (hs httpServer) getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value, _ := hs.db.Load(key)
	if value == nil {
		value = ""
	}
	rsp := struct {
		Data string `json:"data"`
	}{value.(string)}
	err := json.NewEncoder(w).Encode(rsp)
	if err != nil {
		http.Error(w, http.StatusText(http.StatusInternalServerError), http.StatusInternalServerError)
	}
}

func (hs httpServer) joinHandler(w http.ResponseWriter, r *http.Request) {
	followerId := r.URL.Query().Get("followerId")
	followerAddr := r.URL.Query().Get("followerAddr")
	if hs.r.State() != raft.Leader {
		json.NewEncoder(w).Encode(struct {
			Error string `json:"error"`
		}{
			"Not the leader",
		})
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
		return
	}
	err := hs.r.AddVoter(raft.ServerID(followerId), raft.ServerAddress(followerAddr), 0, 0).Error()
	if err != nil {
		log.Printf("Failed to add follower: %s", err)
		http.Error(w, http.StatusText(http.StatusBadRequest), http.StatusBadRequest)
	}

	w.WriteHeader(http.StatusOK)

}

type config struct {
	id       string
	httpPort string
	raftPort string
}

func getConfig() config {
	cfg := config{}

	for i, arg := range os.Args[1:] {
		if arg == "--node-id" {
			cfg.id = os.Args[i+2]
			i++
			continue
		}
		if arg == "--http-port" {
			cfg.httpPort = os.Args[i+2]
			i++
			continue
		}
		if arg == "--raft-port" {
			cfg.raftPort = os.Args[i+2]
			i++
			continue
		}
	}

	if cfg.id == "" {
		log.Fatal("Missing required parameter: --node-id")
	}

	if cfg.raftPort == "" {
		log.Fatal("Missing required parameter: --raft-port")
	}

	if cfg.httpPort == "" {
		log.Fatal("Missing required parameter: --http-port")
	}

	return cfg
}

func main() {
	cfg := getConfig()
	mp := &sync.Map{}
	kv := &db{mp}

	dataDir := "data"
	r, err := setUpRaft(path.Join(dataDir, "raft"+cfg.id), cfg.id, "localhost:"+cfg.raftPort, kv)
	if err != nil {
		log.Fatal(err)
	}
	hs := httpServer{r, mp}

	http.HandleFunc("/set", hs.setHandler)
	http.HandleFunc("/get", hs.getHandler)
	http.HandleFunc("/join", hs.joinHandler)
	http.ListenAndServe(":"+cfg.httpPort, nil)
}
