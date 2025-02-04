package nat

import "fmt"

type DummyNATController struct{}

func (d *DummyNATController) InitNAT() error {
	fmt.Println("InitNAT called")
	return nil
}

func (d *DummyNATController) EnsureNAT(SvcIP, PodIP string) error {
	fmt.Printf("EnsureNAT called with SvcIP: %s, PodIP: %s\n", SvcIP, PodIP)
	return nil
}

func (d *DummyNATController) DeleteNAT(SvcIP, PodIP string) error {
	fmt.Printf("DeleteNAT called with SvcIP: %s, PodIP: %s\n", SvcIP, PodIP)
	return nil
}

func (d *DummyNATController) InitialCleanup(KeepMap map[string]string) error {
	fmt.Println("InitialCleanup called with KeepMap:", KeepMap)
	return nil
}
