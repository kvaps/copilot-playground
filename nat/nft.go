package nat

import (
	"bytes"
	"fmt"
	"net"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

// NFTNATProcessor implements a NATProcessor using nftables.
type NFTNATProcessor struct {
	conn *nftables.Conn

	// Raw table fields
	rawTable      *nftables.Table
	podsSet       *nftables.Set
	svcsSet       *nftables.Set
	preroutingRaw *nftables.Chain

	// Route table fields
	routeTable      *nftables.Table
	snatMap         *nftables.Set // Maps pod IP -> svc IP
	dnatMap         *nftables.Set // Maps svc IP -> pod IP
	outputChain     *nftables.Chain
	preroutingRoute *nftables.Chain
}

// InitNAT loads the base configuration.
// It flushes the existing ruleset, creates a raw table with sets and a prerouting chain
// (and adds lookup rules for pods and services), then creates a route table with SNAT/DNAT maps,
// output and prerouting chains, and adds SNAT/DNAT rules.
func (p *NFTNATProcessor) InitNAT() error {
	// Create a new nftables connection.
	var err error
	p.conn, err = nftables.New()
	if err != nil {
		return fmt.Errorf("could not create nftables connection: %v", err)
	}
	// Flush all existing rules.
	p.conn.FlushRuleset()

	// --- Raw Table Setup ---
	p.rawTable = p.conn.AddTable(&nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   "raw",
	})

	// Create the wholeip_pods set.
	p.podsSet = &nftables.Set{
		Table:   p.rawTable,
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}
	if err := p.conn.AddSet(p.podsSet, nil); err != nil {
		return fmt.Errorf("could not add pods set: %v", err)
	}
	// (Optional) Add a dummy pod IP element.
	// Uncomment the following lines if you wish to preload a pod IP.
	/*
		podIP := net.ParseIP("10.244.0.24").To4()
		if podIP == nil {
			return fmt.Errorf("failed to parse dummy pod IP")
		}
		if err := p.conn.SetAddElements(p.podsSet, []nftables.SetElement{
			{Key: podIP},
		}); err != nil {
			return fmt.Errorf("could not add dummy pod IP to set: %v", err)
		}
	*/

	// Create the wholeip_svcs set.
	p.svcsSet = &nftables.Set{
		Table:   p.rawTable,
		Name:    "wholeip_svcs",
		KeyType: nftables.TypeIPAddr,
	}
	if err := p.conn.AddSet(p.svcsSet, nil); err != nil {
		return fmt.Errorf("could not add services set: %v", err)
	}
	// (Optional) Add a dummy service IP element.
	/*
		svcIP := net.ParseIP("91.223.132.45").To4()
		if svcIP == nil {
			return fmt.Errorf("failed to parse dummy svc IP")
		}
		if err := p.conn.SetAddElements(p.svcsSet, []nftables.SetElement{
			{Key: svcIP},
		}); err != nil {
			return fmt.Errorf("could not add dummy svc IP to set: %v", err)
		}
	*/

	// Create the prerouting chain in the raw table.
	p.preroutingRaw = p.conn.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    p.rawTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRaw,
	})

	// Add a rule to the raw prerouting chain for source addresses.
	p.conn.AddRule(&nftables.Rule{
		Table: p.rawTable,
		Chain: p.preroutingRaw,
		Exprs: []expr.Any{
			// Load source IP (offset 12 in the IP header).
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       12,
				Len:          4,
			},
			// Lookup the source IP in the wholeip_pods set.
			&expr.Lookup{
				SourceRegister: 1,
				SetName:        p.podsSet.Name,
				SetID:          p.podsSet.ID,
			},
			// Do not track connection.
			&expr.Notrack{},
			// Return immediately.
			&expr.Verdict{
				Kind: expr.VerdictReturn,
			},
		},
	})

	// Add a rule to the raw prerouting chain for destination addresses.
	p.conn.AddRule(&nftables.Rule{
		Table: p.rawTable,
		Chain: p.preroutingRaw,
		Exprs: []expr.Any{
			// Load destination IP (offset 16 in the IP header).
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       16,
				Len:          4,
			},
			// Lookup the destination IP in the wholeip_svcs set.
			&expr.Lookup{
				SourceRegister: 1,
				SetName:        p.svcsSet.Name,
				SetID:          p.svcsSet.ID,
			},
			&expr.Notrack{},
			&expr.Verdict{
				Kind: expr.VerdictReturn,
			},
		},
	})

	// --- Route Table Setup ---
	p.routeTable = p.conn.AddTable(&nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   "route",
	})

	// Create the wholeip_snat map (pod IP -> svc IP).
	p.snatMap = &nftables.Set{
		Table:    p.routeTable,
		Name:     "wholeip_snat",
		KeyType:  nftables.TypeIPAddr,
		DataType: nftables.TypeIPAddr,
		IsMap:    true,
	}
	if err := p.conn.AddSet(p.snatMap, nil); err != nil {
		return fmt.Errorf("could not add SNAT map: %v", err)
	}

	// Create the wholeip_dnat map (svc IP -> pod IP).
	p.dnatMap = &nftables.Set{
		Table:    p.routeTable,
		Name:     "wholeip_dnat",
		KeyType:  nftables.TypeIPAddr,
		DataType: nftables.TypeIPAddr,
		IsMap:    true,
	}
	if err := p.conn.AddSet(p.dnatMap, nil); err != nil {
		return fmt.Errorf("could not add DNAT map: %v", err)
	}

	// Create the output chain in the route table.
	p.outputChain = p.conn.AddChain(&nftables.Chain{
		Name:     "output",
		Table:    p.routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATSource + 1),
	})

	// Create the prerouting chain in the route table.
	p.preroutingRoute = p.conn.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    p.routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATDest + 1),
	})

	// Add the SNAT rule to the output chain.
	p.conn.AddRule(&nftables.Rule{
		Table: p.routeTable,
		Chain: p.outputChain,
		Exprs: []expr.Any{
			// Load source IP (offset 12).
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       12,
				Len:          4,
			},
			// Lookup in the SNAT map.
			&expr.Lookup{
				SourceRegister: 1,
				DestRegister:   1,
				SetName:        p.snatMap.Name,
				SetID:          p.snatMap.ID,
				IsDestRegSet:   true,
			},
			// Write the new source IP and update checksum.
			&expr.Payload{
				OperationType:  expr.PayloadWrite,
				SourceRegister: 1,
				Base:           expr.PayloadBaseNetworkHeader,
				Offset:         12,
				Len:            4,
				CsumType:       expr.CsumTypeInet,
				CsumOffset:     10,
				CsumFlags:      unix.NFT_PAYLOAD_L4CSUM_PSEUDOHDR,
			},
		},
	})

	// Add the DNAT rule to the prerouting chain.
	p.conn.AddRule(&nftables.Rule{
		Table: p.routeTable,
		Chain: p.preroutingRoute,
		Exprs: []expr.Any{
			// Load destination IP (offset 16).
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       16,
				Len:          4,
			},
			// Lookup in the DNAT map.
			&expr.Lookup{
				SourceRegister: 1,
				DestRegister:   1,
				SetName:        p.dnatMap.Name,
				SetID:          p.dnatMap.ID,
				IsDestRegSet:   true,
			},
			// Write the new destination IP and update checksum.
			&expr.Payload{
				OperationType:  expr.PayloadWrite,
				SourceRegister: 1,
				Base:           expr.PayloadBaseNetworkHeader,
				Offset:         16,
				Len:            4,
				CsumType:       expr.CsumTypeInet,
				CsumOffset:     10,
				CsumFlags:      unix.NFT_PAYLOAD_L4CSUM_PSEUDOHDR,
			},
		},
	})

	// Commit all the changes.
	if err := p.conn.Flush(); err != nil {
		return fmt.Errorf("failed to commit initial configuration: %v", err)
	}
	return nil
}

// EnsureNAT adds NAT mappings to the SNAT and DNAT maps and ensures the corresponding IPs
// are present in the raw table sets. If a conflicting mapping exists, it returns an error.
func (p *NFTNATProcessor) EnsureNAT(svcIP, podIP string) error {
	parsedSvcIP := net.ParseIP(svcIP).To4()
	if parsedSvcIP == nil {
		return fmt.Errorf("invalid svcIP: %s", svcIP)
	}
	parsedPodIP := net.ParseIP(podIP).To4()
	if parsedPodIP == nil {
		return fmt.Errorf("invalid podIP: %s", podIP)
	}

	// Check for an existing mapping in the SNAT map.
	snatElements, err := p.conn.GetSetElements(p.snatMap)
	if err != nil {
		return fmt.Errorf("failed to get SNAT map elements: %v", err)
	}
	for _, el := range snatElements {
		if bytes.Equal(el.Key, parsedPodIP) {
			if !bytes.Equal(el.Val, parsedSvcIP) {
				return fmt.Errorf("conflict: podIP %s already mapped to a different svcIP", podIP)
			}
			// Mapping already exists.
			return nil
		}
	}

	// Check for an existing mapping in the DNAT map.
	dnatElements, err := p.conn.GetSetElements(p.dnatMap)
	if err != nil {
		return fmt.Errorf("failed to get DNAT map elements: %v", err)
	}
	for _, el := range dnatElements {
		if bytes.Equal(el.Key, parsedSvcIP) {
			if !bytes.Equal(el.Val, parsedPodIP) {
				return fmt.Errorf("conflict: svcIP %s already mapped to a different podIP", svcIP)
			}
			return nil
		}
	}

	// Add the mapping to SNAT and DNAT maps.
	if err := p.conn.SetAddElements(p.snatMap, []nftables.SetElement{
		{Key: parsedPodIP, Val: parsedSvcIP},
	}); err != nil {
		return fmt.Errorf("failed to add SNAT mapping: %v", err)
	}
	if err := p.conn.SetAddElements(p.dnatMap, []nftables.SetElement{
		{Key: parsedSvcIP, Val: parsedPodIP},
	}); err != nil {
		return fmt.Errorf("failed to add DNAT mapping: %v", err)
	}

	// Also ensure that the raw table sets contain the pod and svc IPs.
	if err := p.conn.SetAddElements(p.podsSet, []nftables.SetElement{
		{Key: parsedPodIP},
	}); err != nil {
		return fmt.Errorf("failed to add pod IP to pods set: %v", err)
	}
	if err := p.conn.SetAddElements(p.svcsSet, []nftables.SetElement{
		{Key: parsedSvcIP},
	}); err != nil {
		return fmt.Errorf("failed to add svc IP to svcs set: %v", err)
	}

	if err := p.conn.Flush(); err != nil {
		return fmt.Errorf("failed to commit EnsureNAT changes: %v", err)
	}
	return nil
}

// DeleteNAT removes NAT mappings from the SNAT and DNAT maps.
func (p *NFTNATProcessor) DeleteNAT(svcIP, podIP string) error {
	parsedSvcIP := net.ParseIP(svcIP).To4()
	if parsedSvcIP == nil {
		return fmt.Errorf("invalid svcIP: %s", svcIP)
	}
	parsedPodIP := net.ParseIP(podIP).To4()
	if parsedPodIP == nil {
		return fmt.Errorf("invalid podIP: %s", podIP)
	}

	if err := p.conn.SetDeleteElements(p.snatMap, []nftables.SetElement{
		{Key: parsedPodIP, Val: parsedSvcIP},
	}); err != nil {
		return fmt.Errorf("failed to delete SNAT mapping: %v", err)
	}
	if err := p.conn.SetDeleteElements(p.dnatMap, []nftables.SetElement{
		{Key: parsedSvcIP, Val: parsedPodIP},
	}); err != nil {
		return fmt.Errorf("failed to delete DNAT mapping: %v", err)
	}
	if err := p.conn.Flush(); err != nil {
		return fmt.Errorf("failed to commit DeleteNAT changes: %v", err)
	}
	return nil
}

// InitialCleanup receives a map (keepMap) where keys are svcIP and values are podIP.
// It removes from the system all NAT mappings that do not exactly match an entry in keepMap.
func (p *NFTNATProcessor) InitialCleanup(keepMap map[string]string) error {
	dnatElements, err := p.conn.GetSetElements(p.dnatMap)
	if err != nil {
		return fmt.Errorf("failed to get DNAT map elements: %v", err)
	}

	var toDeleteDNAT []nftables.SetElement
	var toDeleteSNAT []nftables.SetElement

	for _, el := range dnatElements {
		currentSvcIP := net.IP(el.Key).String()
		currentPodIP := net.IP(el.Val).String()
		if keepPodIP, exists := keepMap[currentSvcIP]; !exists || keepPodIP != currentPodIP {
			toDeleteDNAT = append(toDeleteDNAT, nftables.SetElement{Key: el.Key, Val: el.Val})
			toDeleteSNAT = append(toDeleteSNAT, nftables.SetElement{Key: el.Val, Val: el.Key})
		}
	}

	if len(toDeleteDNAT) > 0 {
		if err := p.conn.SetDeleteElements(p.dnatMap, toDeleteDNAT); err != nil {
			return fmt.Errorf("failed to delete DNAT mappings during cleanup: %v", err)
		}
		if err := p.conn.SetDeleteElements(p.snatMap, toDeleteSNAT); err != nil {
			return fmt.Errorf("failed to delete SNAT mappings during cleanup: %v", err)
		}
		if err := p.conn.Flush(); err != nil {
			return fmt.Errorf("failed to commit InitialCleanup changes: %v", err)
		}
	}
	return nil
}
