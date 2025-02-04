// nat/interface.go
package nat

type NATController interface {
	InitNAT() error
	EnsureNAT(SvcIP, PodIP string) error
	DeleteNAT(SvcIP, PodIP string) error
	InitialCleanup(Keep map[string]string) error
}
