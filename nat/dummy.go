package nat

import "fmt"

type DummyNATProcessor struct{}

func (d *DummyNATProcessor) InitNAT() error {
	fmt.Println("InitNAT called")
	return nil
}

func (d *DummyNATProcessor) EnsureNAT(SvcIP, PodIP string) error {
	fmt.Printf("EnsureNAT called with SvcIP: %s, PodIP: %s\n", SvcIP, PodIP)
	return nil
}

func (d *DummyNATProcessor) DeleteNAT(SvcIP, PodIP string) error {
	fmt.Printf("DeleteNAT called with SvcIP: %s, PodIP: %s\n", SvcIP, PodIP)
	return nil
}

func (d *DummyNATProcessor) InitialCleanup(KeepMap map[string]string) error {
	fmt.Println("InitialCleanup called with KeepMap:", KeepMap)
	return nil
}
