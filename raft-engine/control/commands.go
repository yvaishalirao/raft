package control

import (
	"encoding/json"
	"net/http"
	"time"
)

type statusResponse struct {
	Status  string `json:"status"`
	Message string `json:"message,omitempty"`
}

func writeOK(w http.ResponseWriter) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(statusResponse{Status: "ok"})
}

func writeError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(statusResponse{Status: "error", Message: msg})
}

func requirePost(w http.ResponseWriter, r *http.Request) bool {
	if r.Method != http.MethodPost {
		writeError(w, http.StatusMethodNotAllowed, "POST required")
		return false
	}
	return true
}

func (c *ControlServer) handleKill(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	c.faultState.SetKilled(c.nodeID, true)
	writeOK(w)
}

// handleRestart un-kills the transport AND resets the node's own term/
// votedFor via Node.Restart — a restart is not just un-killing the
// transport.
func (c *ControlServer) handleRestart(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	c.faultState.SetKilled(c.nodeID, false)

	c.mu.Lock()
	node := c.node
	c.mu.Unlock()
	if node != nil {
		node.Restart()
	}
	writeOK(w)
}

type peersRequest struct {
	Peers []string `json:"peers"`
}

func (c *ControlServer) handlePartition(w http.ResponseWriter, r *http.Request) {
	c.togglePartition(w, r, true)
}

func (c *ControlServer) handleUnpartition(w http.ResponseWriter, r *http.Request) {
	c.togglePartition(w, r, false)
}

func (c *ControlServer) togglePartition(w http.ResponseWriter, r *http.Request, partitioned bool) {
	if !requirePost(w, r) {
		return
	}
	var req peersRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if len(req.Peers) == 0 {
		writeError(w, http.StatusBadRequest, "peers must be non-empty")
		return
	}
	for _, peer := range req.Peers {
		if peer == "" {
			writeError(w, http.StatusBadRequest, "peer id must not be empty")
			return
		}
	}

	for _, peer := range req.Peers {
		c.faultState.SetPartition(c.nodeID, peer, partitioned)
		c.faultState.SetPartition(peer, c.nodeID, partitioned)
	}
	writeOK(w)
}

type latencyRequest struct {
	Peer string `json:"peer"`
	MS   int    `json:"ms"`
}

func (c *ControlServer) handleLatency(w http.ResponseWriter, r *http.Request) {
	if !requirePost(w, r) {
		return
	}
	var req latencyRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}
	if req.Peer == "" {
		writeError(w, http.StatusBadRequest, "peer must not be empty")
		return
	}
	if req.MS < 0 {
		writeError(w, http.StatusBadRequest, "ms must be non-negative")
		return
	}

	c.faultState.SetDelay(c.nodeID, req.Peer, time.Duration(req.MS)*time.Millisecond)
	writeOK(w)
}
