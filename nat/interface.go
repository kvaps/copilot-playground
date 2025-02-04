// nat/interface.go
package nat

type NATProcessor interface {
	InitNAT() error
	EnsureNAT(SvcIP, PodIP string) error
	DeleteNAT(SvcIP, PodIP string) error
	InitialCleanup(KeepMap map[string]string) error
}
