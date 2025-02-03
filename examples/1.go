package main

import (
	"log"
	"net"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
)

func main() {
	c := &nftables.Conn{}

	// Flush all tables first
	c.FlushRuleset()

	// Create raw table
	rawTable := c.AddTable(&nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   "raw",
	})

	// Create wholeip_pods set
	podsSet := &nftables.Set{
		Table:   rawTable,
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}
	if err := c.AddSet(podsSet, nil); err != nil {
		log.Fatalf("could not add pods set: %v", err)
	}

	// Add element to pods set
	podIP := net.ParseIP("10.244.0.24").To4()
	err := c.SetAddElements(podsSet, []nftables.SetElement{
		{Key: podIP},
	})
	if err != nil {
		log.Fatalf("could not add pod IP to set: %v", err)
	}

	// Create wholeip_svcs set
	svcsSet := &nftables.Set{
		Table:   rawTable,
		Name:    "wholeip_svcs",
		KeyType: nftables.TypeIPAddr,
	}
	if err := c.AddSet(svcsSet, nil); err != nil {
		log.Fatalf("could not add services set: %v", err)
	}

	// Add element to services set
	svcIP := net.ParseIP("91.223.132.45").To4()
	err = c.SetAddElements(svcsSet, []nftables.SetElement{
		{Key: svcIP},
	})
	if err != nil {
		log.Fatalf("could not add service IP to set: %v", err)
	}

	// Create prerouting chain in raw table
	preroutingRaw := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    rawTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRaw,
	})

	// Add rules to raw prerouting chain for source address
	c.AddRule(&nftables.Rule{
		Table: rawTable,
		Chain: preroutingRaw,
		Exprs: []expr.Any{
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       12,
				Len:          4,
			},
			&expr.Lookup{
				SourceRegister: 1,
				SetName:        "wholeip_pods",
				SetID:          podsSet.ID,
			},
			&expr.Notrack{},
			&expr.Verdict{
				Kind: expr.VerdictReturn,
			},
		},
	})

	// Add rules to raw prerouting chain for destination address
	c.AddRule(&nftables.Rule{
		Table: rawTable,
		Chain: preroutingRaw,
		Exprs: []expr.Any{
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       16,
				Len:          4,
			},
			&expr.Lookup{
				SourceRegister: 1,
				SetName:        "wholeip_svcs",
				SetID:          svcsSet.ID,
			},
			&expr.Notrack{},
			&expr.Verdict{
				Kind: expr.VerdictReturn,
			},
		},
	})

	// ----------------------

	// Create route table
	routeTable := c.AddTable(&nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   "route",
	})

	// Create wholeip_snat map
	snatMap := &nftables.Set{
		Table:    routeTable,
		Name:     "wholeip_snat",
		KeyType:  nftables.TypeIPAddr,
		DataType: nftables.TypeIPAddr,
		IsMap:    true,
	}
	if err := c.AddSet(snatMap, nil); err != nil {
		log.Fatalf("could not add SNAT map: %v", err)
	}

	// Add elements to SNAT map
	err = c.SetAddElements(snatMap, []nftables.SetElement{
		{
			Key: podIP,
			Val: svcIP,
		},
	})
	if err != nil {
		log.Fatalf("could not add SNAT mapping: %v", err)
	}

	// Create wholeip_dnat map
	dnatMap := &nftables.Set{
		Table:    routeTable,
		Name:     "wholeip_dnat",
		KeyType:  nftables.TypeIPAddr,
		DataType: nftables.TypeIPAddr,
		IsMap:    true,
	}
	if err := c.AddSet(dnatMap, nil); err != nil {
		log.Fatalf("could not add DNAT map: %v", err)
	}

	// Add elements to DNAT map
	err = c.SetAddElements(dnatMap, []nftables.SetElement{
		{
			Key: svcIP,
			Val: podIP,
		},
	})
	if err != nil {
		log.Fatalf("could not add DNAT mapping: %v", err)
	}

	// Create output chain in route table
	output := c.AddChain(&nftables.Chain{
		Name:     "output",
		Table:    routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATSource + 1),
	})

	// Create prerouting chain in route table
	preroutingRoute := c.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATDest + 1),
	})

	// Add SNAT rule
	c.AddRule(&nftables.Rule{
		Table: routeTable,
		Chain: output,
		Exprs: []expr.Any{
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       12,
				Len:          4,
			},
			&expr.Lookup{
				SourceRegister: 1,
				DestRegister:   1,
				SetName:        "wholeip_snat",
				SetID:          snatMap.ID,
				IsDestRegSet:   true,
			},
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

	// Add DNAT rule
	c.AddRule(&nftables.Rule{
		Table: routeTable,
		Chain: preroutingRoute,
		Exprs: []expr.Any{
			&expr.Payload{
				DestRegister: 1,
				Base:         expr.PayloadBaseNetworkHeader,
				Offset:       16,
				Len:          4,
			},
			&expr.Lookup{
				SourceRegister: 1,
				DestRegister:   1,
				SetName:        "wholeip_dnat",
				SetID:          dnatMap.ID,
				IsDestRegSet:   true,
			},
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

	// Commit the changes
	if err := c.Flush(); err != nil {
		log.Fatalf("failed to commit changes: %v", err)
	}
}
