package controllers

import (
	"context"
	"fmt"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	log = ctrl.Log.WithName("ep-controller")
)

type EndpointsController struct {
	RESTClient *rest.RESTClient
}

func (c EndpointsController) Start(ctx context.Context) error {
	log.Info("starting vmi routes controller")

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
	log.Info("syncronizing")

	//syncronize the cache before starting to process
	if !cache.WaitForCacheSync(stopper, informer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for caches to sync"))
		log.Info("syncronization failed")
		return fmt.Errorf("syncronization failed")
	}
	log.Info("syncronization completed")

	log.Info("create chains")
	//if err := c.setupNFTChains(); err != nil {
	//	return fmt.Errorf("failed to create chains: %w", err)
	//}

	log.Info("running cleanup for removed services")
	//if err := c.cleanupRemovedServices(informer); err != nil {
	//	return fmt.Errorf("failed to cleanup removed services: %w", err)
	//}
	log.Info("cleanup of removed services completed")

	<-ctx.Done()
	log.Info("shutting down endpoints controller")

	return nil
}

func (c EndpointsController) addFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	fmt.Println("add", ep.GetNamespace(), ep.GetName())
}
func (c EndpointsController) deleteFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	fmt.Println("del", ep.GetNamespace(), ep.GetName())
}

func (c EndpointsController) updateFunc(oldObj, newObj interface{}) {
	ep, ok := newObj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	fmt.Println("update", ep.GetNamespace(), ep.GetName())
}
