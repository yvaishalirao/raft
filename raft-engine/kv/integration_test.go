package kv

import (
	"testing"
	"time"
)

func TestKV_WritesAndReadsAcrossLeaderChanges(t *testing.T) {
	stores, nodes, cancels := buildKVCluster(t, 5)
	defer cancelAll(cancels)

	leader := waitForLeader(t, nodes, 2*time.Second)
	leaderStore := storeFor(stores, nodes, leader)

	first := map[string]string{"a": "1", "b": "2", "c": "3"}
	for k, v := range first {
		if err := leaderStore.HandleSet(k, v); err != nil {
			t.Fatalf("SET %s=%s on original leader failed: %v", k, v, err)
		}
	}

	killedID := leader.ID()
	killedIdx := indexOf(nodes, leader)
	cancels[killedIdx]()

	newLeader := waitForLeaderExcluding(t, nodes, killedID, 3*time.Second)
	newLeaderStore := storeFor(stores, nodes, newLeader)

	second := map[string]string{"d": "4", "e": "5"}
	for k, v := range second {
		if err := newLeaderStore.HandleSet(k, v); err != nil {
			t.Fatalf("SET %s=%s on new leader failed: %v", k, v, err)
		}
	}

	all := map[string]string{}
	for k, v := range first {
		all[k] = v
	}
	for k, v := range second {
		all[k] = v
	}

	deadline := time.Now().Add(2 * time.Second)
	var mismatches map[string]string
	for time.Now().Before(deadline) {
		mismatches = nil
		for i, n := range nodes {
			if i == killedIdx {
				continue
			}
			for k, want := range all {
				if got, ok := stores[i].Get(k); !ok || got != want {
					if mismatches == nil {
						mismatches = map[string]string{}
					}
					mismatches[n.ID()+"/"+k] = got
				}
			}
		}
		if mismatches == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if mismatches != nil {
		t.Fatalf("keys did not converge across surviving nodes: %+v", mismatches)
	}

	for k, want := range second {
		res, err := newLeaderStore.HandleGet(k, "linearizable")
		if err != nil {
			t.Fatalf("linearizable GET %s failed: %v", k, err)
		}
		if !res.OK || res.Value != want {
			t.Fatalf("linearizable GET %s: got value=%q ok=%v, want %q", k, res.Value, res.OK, want)
		}
	}
}
