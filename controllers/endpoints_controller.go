package controllers

import (
	"context"
	"fmt"
	"log"
	"net"
	"time"

	"github.com/google/nftables"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
)

type EndpointsController struct {
	RESTClient *rest.RESTClient
	nftConn    *nftables.Conn
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
	c.nftConn = &nftables.Conn{}

	// Load initial nftables configuration here
	// Example: Create necessary tables, sets, chains, and rules

	return nil
}

func (c EndpointsController) cleanupRemovedServices(informer cache.SharedIndexInformer) error {
	// Logic to cleanup removed services
	// Example: Iterate over cached items and clean up nftables rules for non-existent services

	for _, obj := range informer.GetStore().List() {
		ep, ok := obj.(*v1.Endpoints)
		if !ok {
			continue
		}
		// Check if the endpoint still exists in the cluster
		if !c.endpointExists(ep) {
			c.removeNftablesRules(ep)
		}
	}

	return nil
}

func (c EndpointsController) endpointExists(ep *v1.Endpoints) bool {
	// Logic to check if the endpoint still exists in the cluster
	// Example: Query the Kubernetes API to verify the existence of the endpoint
	return true
}

func (c EndpointsController) updateNftablesRules(ep *v1.Endpoints) {
	// Implement logic to add/update nftables rules here
	// Example: add IP to set
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			c.addIPToSet(addr.IP)
		}
	}
}

func (c EndpointsController) removeNftablesRules(ep *v1.Endpoints) {
	// Implement logic to remove nftables rules here
	// Example: remove IP from set
	for _, subset := range ep.Subsets {
		for _, addr := range subset.Addresses {
			c.removeIPFromSet(addr.IP)
		}
	}
}

func (c EndpointsController) addIPToSet(ip string) {
	// Example logic to add IP to nftables set
	podIP := net.ParseIP(ip).To4()
	err := c.nftConn.SetAddElements(&nftables.Set{
		Table:   &nftables.Table{Name: "raw"},
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}, []nftables.SetElement{{Key: podIP}})
	if err != nil {
		log.Fatalf("could not add IP to set: %v", err)
	}
}

func (c EndpointsController) removeIPFromSet(ip string) {
	// Example logic to remove IP from nftables set
	podIP := net.ParseIP(ip).To4()
	err := c.nftConn.SetDeleteElements(&nftables.Set{
		Table:   &nftables.Table{Name: "raw"},
		Name:    "wholeip_pods",
		KeyType: nftables.TypeIPAddr,
	}, []nftables.SetElement{{Key: podIP}})
	if err != nil {
		log.Fatalf("could not remove IP from set: %v", err)
	}
}
