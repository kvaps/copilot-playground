package controllers

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/google/nftables"
	"github.com/google/nftables/expr"
	"golang.org/x/sys/unix"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
)

type EndpointsController struct {
	RESTClient *rest.RESTClient
	NFTConn    *nftables.Conn
}

func (c EndpointsController) Start(ctx context.Context) error {
	log := ctrl.Log.WithName("ep-controller")
	log.Info("starting endpoints controller")

	lw := cache.NewListWatchFromClient(c.RESTClient, "endpoints", v1.NamespaceAll, fields.Everything())
	informer := cache.NewSharedIndexInformer(
		lw,
		&v1.Endpoints{},
		12*time.Hour,
		cache.Indexers{
			"namespace_name": func(obj interface{}) ([]string, error) {
				ep, ok := obj.(*v1.Endpoints)
				if !ok {
					return nil, fmt.Errorf("object is not *v1.Endpoints")
				}
				return []string{ep.Namespace + "/" + ep.Name}, nil
			},
		},
	)

	informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addFunc,
		DeleteFunc: c.deleteFunc,
		UpdateFunc: c.updateFunc,
	})

	stopper := make(chan struct{})
	defer close(stopper)
	defer utilruntime.HandleCrash()
	go informer.Run(stopper)
	log.Info("synchronizing")

	if !cache.WaitForCacheSync(stopper, informer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		log.Info("synchronization failed")
		return fmt.Errorf("synchronization failed")
	}
	log.Info("synchronization completed")

	log.Info("create chains")
	if err := c.setupNFTChains(); err != nil {
		return fmt.Errorf("failed to create chains: %w", err)
	}

	log.Info("running cleanup for removed services")
	if err := c.cleanupRemovedServices(informer); err != nil {
		return fmt.Errorf("failed to cleanup removed services: %w", err)
	}
	log.Info("cleanup of removed services completed")

	<-ctx.Done()
	log.Info("shutting down endpoints controller")

	return nil
}

func (c EndpointsController) addFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		return
	}
	fmt.Println("add", ep.GetNamespace(), ep.GetName())
	c.updateNftablesRules(ep)
}

func (c EndpointsController) deleteFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		return
	}
	fmt.Println("del", ep.GetNamespace(), ep.GetName())
	c.removeNftablesRules(ep)
}

func (c EndpointsController) updateFunc(oldObj, newObj interface{}) {
	ep, ok := newObj.(*v1.Endpoints)
	if !ok {
		return
	}
	fmt.Println("update", ep.GetNamespace(), ep.GetName())
	c.updateNftablesRules(ep)
}

func (c EndpointsController) setupNFTChains() error {
	// Initialize nftables connection
	c.NFTConn = &nftables.Conn{}

	// Flush all tables first
	c.NFTConn.FlushRuleset()

	// Create raw table
	rawTable := c.NFTConn.AddTable(&nftables.Table{
		Family: nftables.TableFamilyIPv4,
		Name:   "raw",
	})

	// Create wholeip_pods set
	podsSet := &nftables.Set{
		Table:   rawTable,
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}
	if err := c.NFTConn.AddSet(podsSet, nil); err != nil {
		return fmt.Errorf("could not add pods set: %w", err)
	}

	// Create wholeip_svcs set
	svcsSet := &nftables.Set{
		Table:   rawTable,
		Name:    "wholeip_svcs",
		KeyType: nftables.TypeIPAddr,
	}
	if err := c.NFTConn.AddSet(svcsSet, nil); err != nil {
		return fmt.Errorf("could not add services set: %w", err)
	}

	// Create prerouting chain in raw table
	preroutingRaw := c.NFTConn.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    rawTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRaw,
	})

	// Add rules to raw prerouting chain for source address
	c.NFTConn.AddRule(&nftables.Rule{
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
	c.NFTConn.AddRule(&nftables.Rule{
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

	// Create route table
	routeTable := c.NFTConn.AddTable(&nftables.Table{
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
	if err := c.NFTConn.AddSet(snatMap, nil); err != nil {
		return fmt.Errorf("could not add SNAT map: %w", err)
	}

	// Create wholeip_dnat map
	dnatMap := &nftables.Set{
		Table:    routeTable,
		Name:     "wholeip_dnat",
		KeyType:  nftables.TypeIPAddr,
		DataType: nftables.TypeIPAddr,
		IsMap:    true,
	}
	if err := c.NFTConn.AddSet(dnatMap, nil); err != nil {
		return fmt.Errorf("could not add DNAT map: %w", err)
	}

	// Create output chain in route table
	output := c.NFTConn.AddChain(&nftables.Chain{
		Name:     "output",
		Table:    routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPostrouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATSource + 1),
	})

	// Create prerouting chain in route table
	preroutingRoute := c.NFTConn.AddChain(&nftables.Chain{
		Name:     "prerouting",
		Table:    routeTable,
		Type:     nftables.ChainTypeFilter,
		Hooknum:  nftables.ChainHookPrerouting,
		Priority: nftables.ChainPriorityRef(*nftables.ChainPriorityNATDest + 1),
	})

	// Add SNAT rule
	c.NFTConn.AddRule(&nftables.Rule{
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
	c.NFTConn.AddRule(&nftables.Rule{
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
	if err := c.NFTConn.Flush(); err != nil {
		return fmt.Errorf("failed to commit changes: %w", err)
	}

	return nil
}

func (c EndpointsController) cleanupRemovedServices(informer cache.SharedIndexInformer) error {
	for _, obj := range informer.GetStore().List() {
		ep, ok := obj.(*v1.Endpoints)
		if !ok {
			continue
		}
		if !c.endpointExists(ep) {
			c.removeNftablesRules(ep)
		}
	}

	return nil
}

func (c EndpointsController) endpointExists(ep *v1.Endpoints) bool {
	return true
}

func (c EndpointsController) updateNftablesRules(ep *v1.Endpoints) {
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			c.addIPToSet(addr.IP)
		}
	}
}

func (c EndpointsController) removeNftablesRules(ep *v1.Endpoints) {
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			c.removeIPFromSet(addr.IP)
		}
	}
}

func (c EndpointsController) addIPToSet(ip string) {
	podIP := net.ParseIP(ip).To4()
	err := c.NFTConn.SetAddElements(&nftables.Set{
		Table:   &nftables.Table{Name: "raw"},
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}, []nftables.SetElement{{Key: podIP}})
	if err != nil {
		log.Fatalf("could not add IP to set: %v", err)
	}
}

func (c EndpointsController) removeIPFromSet(ip string) {
	podIP := net.ParseIP(ip).To4()
	err := c.NFTConn.SetDeleteElements(&nftables.Set{
		Table:   &nftables.Table{Name: "raw"},
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}, []nftables.SetElement{{Key: podIP}})
	if err != nil {
		log.Fatalf("could not remove IP from set: %v", err)
	}
}
