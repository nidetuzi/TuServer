package cluster

import (
	"context"
	"fmt"
	"testing"

	"github.com/cloudwego/kitex/pkg/discovery"
)

func TestGatesEmptyRegistry(t *testing.T) {
	n := &Nodes{resolver: discovery.SynthesizedResolver{
		ResolveFunc: func(context.Context, string) (discovery.Result, error) {
			return discovery.Result{}, fmt.Errorf("no instance remains for Gate")
		},
	}}
	got, err := n.Gates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("没有实例时应为空列表: %v", got)
	}
}

func TestGatesKeepsQueryError(t *testing.T) {
	n := &Nodes{resolver: discovery.SynthesizedResolver{
		ResolveFunc: func(context.Context, string) (discovery.Result, error) {
			return discovery.Result{}, fmt.Errorf("etcd down")
		},
	}}
	if _, err := n.Gates(context.Background()); err == nil {
		t.Fatal("查询失败不能当成没有网关")
	}
}

func TestGatesListsAddresses(t *testing.T) {
	n := &Nodes{resolver: discovery.SynthesizedResolver{
		ResolveFunc: func(context.Context, string) (discovery.Result, error) {
			return discovery.Result{Instances: []discovery.Instance{
				discovery.NewInstance("tcp", "127.0.0.1:9001", 10, nil),
			}}, nil
		},
	}}
	got, err := n.Gates(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0] != "127.0.0.1:9001" {
		t.Fatalf("地址不对: %v", got)
	}
}
