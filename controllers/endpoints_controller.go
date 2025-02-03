package controllers

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	ctrl "sigs.k8s.io/controller-runtime"
)

var (
	log = ctrl.Log.WithName("svc-controller")
)

type ServiceEndpoints struct {
	Service  *v1.Service
	Endpoint *v1.Endpoints
}

type ServicesController struct {
	Clientset    *kubernetes.Clientset
	ServiceMap   map[string]*ServiceEndpoints
	ServiceMutex sync.Mutex
}

func (c ServicesController) Start(ctx context.Context) error {
	log.Info("starting services controller")

	c.ServiceMap = make(map[string]*ServiceEndpoints)

	// Create informer for services
	serviceLW := cache.NewListWatchFromClient(c.Clientset.CoreV1().RESTClient(), "services", v1.NamespaceAll, fields.Everything())
	serviceInformer := cache.NewSharedIndexInformer(
		serviceLW,
		&v1.Service{},
		12*time.Hour,
		cache.Indexers{
			"namespace_name": func(obj interface{}) ([]string, error) {
				svc, ok := obj.(*v1.Service)
				if !ok {
					return nil, fmt.Errorf("object is not *v1.Service")
				}
				return []string{svc.Namespace + "/" + svc.Name}, nil
			},
		},
	)

	serviceInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addServiceFunc,
		DeleteFunc: c.deleteServiceFunc,
		UpdateFunc: c.updateServiceFunc,
	})

	stopper := make(chan struct{})
	defer close(stopper)
	defer utilruntime.HandleCrash()

	// Start service informer and wait for its cache to sync
	go serviceInformer.Run(stopper)
	log.Info("synchronizing services")

	if !cache.WaitForCacheSync(stopper, serviceInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for services cache to sync"))
		log.Info("synchronization of services failed")
		return fmt.Errorf("synchronization of services failed")
	}
	log.Info("services synchronization completed")

	// Start loading endpoints after services are fully loaded
	endpointsLW := cache.NewListWatchFromClient(c.Clientset.CoreV1().RESTClient(), "endpoints", v1.NamespaceAll, fields.Everything())
	endpointsInformer := cache.NewSharedIndexInformer(
		endpointsLW,
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

	endpointsInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    c.addEndpointFunc,
		DeleteFunc: c.deleteEndpointFunc,
		UpdateFunc: c.updateEndpointFunc,
	})

	go endpointsInformer.Run(stopper)
	log.Info("synchronizing endpoints")

	if !cache.WaitForCacheSync(stopper, endpointsInformer.HasSynced) {
		utilruntime.HandleError(fmt.Errorf("Timed out waiting for endpoints cache to sync"))
		log.Info("synchronization of endpoints failed")
		return fmt.Errorf("synchronization of endpoints failed")
	}
	log.Info("endpoints synchronization completed")

	log.Info("running cleanup for removed services")
	if err := c.cleanupRemovedServices(); err != nil {
		return fmt.Errorf("failed to cleanup removed services: %w", err)
	}
	log.Info("cleanup of removed services completed")

	<-ctx.Done()
	log.Info("shutting down services controller")

	return nil
}

func (c *ServicesController) addServiceFunc(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	if val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]; ok && val == "true" {
		c.ServiceMutex.Lock()
		defer c.ServiceMutex.Unlock()
		c.ServiceMap[svc.Namespace+"/"+svc.Name] = &ServiceEndpoints{Service: svc}

		// Fetch the corresponding endpoint if it exists
		ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
		if err == nil {
			c.ServiceMap[svc.Namespace+"/"+svc.Name].Endpoint = ep
		}
	}
	fmt.Println("add service", svc.GetNamespace(), svc.GetName())
	// TODO
	if sm, ok := c.ServiceMap[svc.Namespace+"/"+svc.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

func (c *ServicesController) deleteServiceFunc(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
	fmt.Println("delete service", svc.GetNamespace(), svc.GetName())
	// TODO
	if sm, ok := c.ServiceMap[svc.Namespace+"/"+svc.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

func (c *ServicesController) updateServiceFunc(oldObj, newObj interface{}) {
	svc, ok := newObj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	if val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]; ok && val == "true" {
		if se, exists := c.ServiceMap[svc.Namespace+"/"+svc.Name]; exists {
			se.Service = svc
		} else {
			c.ServiceMap[svc.Namespace+"/"+svc.Name] = &ServiceEndpoints{Service: svc}

			// Fetch the corresponding endpoint if it exists
			ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
			if err == nil {
				c.ServiceMap[svc.Namespace+"/"+svc.Name].Endpoint = ep
			}
		}
	} else {
		delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
	}
	fmt.Println("update service", svc.GetNamespace(), svc.GetName())

	// TODO
	if sm, ok := c.ServiceMap[svc.Namespace+"/"+svc.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

func (c *ServicesController) addEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	if se, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]; exists {
		se.Endpoint = ep
	} else {
		c.ServiceMap[ep.Namespace+"/"+ep.Name] = &ServiceEndpoints{Endpoint: ep}
	}
	fmt.Println("add endpoint", ep.GetNamespace(), ep.GetName())
	// TODO
	if sm, ok := c.ServiceMap[ep.Namespace+"/"+ep.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

func (c *ServicesController) deleteEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	if _, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]; exists {
		c.ServiceMap[ep.Namespace+"/"+ep.Name].Endpoint = nil
	}
	fmt.Println("delete endpoint", ep.GetNamespace(), ep.GetName())
	// TODO
	if sm, ok := c.ServiceMap[ep.Namespace+"/"+ep.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

func (c *ServicesController) updateEndpointFunc(oldObj, newObj interface{}) {
	ep, ok := newObj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	if se, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]; exists {
		se.Endpoint = ep
	} else {
		c.ServiceMap[ep.Namespace+"/"+ep.Name] = &ServiceEndpoints{Endpoint: ep}
	}
	fmt.Println("update endpoint", ep.GetNamespace(), ep.GetName())
	// TODO
	if sm, ok := c.ServiceMap[ep.Namespace+"/"+ep.Name]; ok && sm.Service != nil && sm.Endpoint != nil {
		if len(sm.Service.Status.LoadBalancer.Ingress) > 0 && len(sm.Endpoint.Subsets) > 0 && len(sm.Endpoint.Subsets[0].Addresses) > 0 {
			fmt.Println(sm.Service.Status.LoadBalancer.Ingress[0].IP, sm.Endpoint.Subsets[0].Addresses[0].IP)
		}
	}
}

// Placeholder for cleanupRemovedServices
func (c *ServicesController) cleanupRemovedServices() error {
	// Placeholder logic for removing services not in the map
	return nil
}
