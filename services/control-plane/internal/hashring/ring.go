package hashring

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

type Member struct {
	ID      string
	Address string
}

type virtualNode struct {
	hash   uint64
	member Member
}

type Ring struct {
	nodes []virtualNode
}

func New(members []Member, virtualNodes int) (*Ring, error) {
	if virtualNodes <= 0 {
		return nil, errors.New("virtual node count must be positive")
	}
	clean := normalize(members)
	if len(clean) == 0 {
		return nil, errors.New("ring requires at least one member")
	}
	nodes := make([]virtualNode, 0, len(clean)*virtualNodes)
	for _, member := range clean {
		for i := 0; i < virtualNodes; i++ {
			key := fmt.Sprintf("%s#%d", member.ID, i)
			nodes = append(nodes, virtualNode{hash: hash64(key), member: member})
		}
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].hash == nodes[j].hash {
			return nodes[i].member.ID < nodes[j].member.ID
		}
		return nodes[i].hash < nodes[j].hash
	})
	return &Ring{nodes: nodes}, nil
}

func (r *Ring) Owner(key string) (Member, bool) {
	if r == nil || len(r.nodes) == 0 {
		return Member{}, false
	}
	h := hash64(key)
	idx := sort.Search(len(r.nodes), func(i int) bool {
		return r.nodes[i].hash >= h
	})
	if idx == len(r.nodes) {
		idx = 0
	}
	return r.nodes[idx].member, true
}

func Fingerprint(members []Member) string {
	clean := normalize(members)
	parts := make([]string, 0, len(clean))
	for _, member := range clean {
		parts = append(parts, member.ID+"="+member.Address)
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(sum[:])
}

func normalize(members []Member) []Member {
	clean := make([]Member, 0, len(members))
	seen := map[string]bool{}
	for _, member := range members {
		if member.ID == "" || member.Address == "" || seen[member.ID] {
			continue
		}
		seen[member.ID] = true
		clean = append(clean, member)
	}
	sort.Slice(clean, func(i, j int) bool {
		if clean[i].ID == clean[j].ID {
			return clean[i].Address < clean[j].Address
		}
		return clean[i].ID < clean[j].ID
	})
	return clean
}

func hash64(value string) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return h.Sum64()
}

