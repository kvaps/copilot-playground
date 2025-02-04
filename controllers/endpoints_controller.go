package controllers

import (
	"context"
	"fmt"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
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
	if val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]; !ok || val != "true" {
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()

	// Fetch the corresponding endpoint if it exists
	ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
	if err != nil {
		// TODO error
		return
	}

	if len(svc.Status.LoadBalancer.Ingress) == 0 || svc.Status.LoadBalancer.Ingress[0].IP == "" {
		return
	}
	if len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 || ep.Subsets[0].Addresses[0].IP == "" {
		return
	}

	c.ensureRules(svc.Namespace, svc.Name, svc.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)

	if _, exists := c.ServiceMap[svc.Namespace+"/"+svc.Name]; exists {
		// Service already added
		return
	}
	c.ServiceMap[svc.Namespace+"/"+svc.Name] = &ServiceEndpoints{Service: svc}
	c.ServiceMap[svc.Namespace+"/"+svc.Name].Endpoint = ep
}

func (c *ServicesController) deleteServiceFunc(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	se, exists := c.ServiceMap[svc.Namespace+"/"+svc.Name]
	if !exists {
		// Service already deleted
		return
	}

	if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
		return
	}
	if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
		return
	}

	c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
	delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
}

func (c *ServicesController) updateServiceFunc(oldObj, newObj interface{}) {
	svc, ok := newObj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	if val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]; !ok || val != "true" {
		if se, exists := c.ServiceMap[svc.Namespace+"/"+svc.Name]; exists {
			if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
				return
			}
			if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
				return
			}
			c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
			delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
			return
		}
	}

	if val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]; !ok || val != "true" {
		return
	}

	se, exists := c.ServiceMap[svc.Namespace+"/"+svc.Name]

	// Service have no IP
	if len(svc.Status.LoadBalancer.Ingress) == 0 || svc.Status.LoadBalancer.Ingress[0].IP == "" {
		if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
			return
		}
		if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
			return
		}
		c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
		return
	}

	// Fetch the corresponding endpoint if it exists
	ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		// TODO error
		return
	}

	// Endpoint have no IP
	if len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 || ep.Subsets[0].Addresses[0].IP == "" {
		if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
			return
		}
		if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
			return
		}
		c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		delete(c.ServiceMap, svc.Namespace+"/"+svc.Name)
		return
	}

	if ep == nil {
		// TODO error
		return
	}

	c.ensureRules(svc.Namespace, svc.Name, svc.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)

	if exists {
		se.Service = svc
	} else {
		c.ServiceMap[svc.Namespace+"/"+svc.Name] = &ServiceEndpoints{Service: svc}
	}
	c.ServiceMap[svc.Namespace+"/"+svc.Name].Endpoint = ep

}

func (c *ServicesController) addEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	se, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]
	if !exists {
		// Service is not managed by us
		return
	} else {
		se.Endpoint = ep
	}

	if se.Service == nil || len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
		return
	}
	if len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 || ep.Subsets[0].Addresses[0].IP == "" {
		return
	}

	c.ensureRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)
}

func (c *ServicesController) deleteEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()

	se, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]
	if !exists {
		// Service is not managed by us
		return
	}

	if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
		return
	}
	if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
		return
	}

	c.deleteRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
	c.ServiceMap[ep.Namespace+"/"+ep.Name].Endpoint = nil
}

func (c *ServicesController) updateEndpointFunc(oldObj, newObj interface{}) {
	ep, ok := newObj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	c.ServiceMutex.Lock()
	defer c.ServiceMutex.Unlock()
	se, exists := c.ServiceMap[ep.Namespace+"/"+ep.Name]
	if !exists {
		// Service is not managed by us
		return
	}

	if len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 || ep.Subsets[0].Addresses[0].IP == "" {
		if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
			return
		}
		if len(se.Endpoint.Subsets) == 0 || len(se.Endpoint.Subsets[0].Addresses) == 0 || se.Endpoint.Subsets[0].Addresses[0].IP == "" {
			return
		}
		c.deleteRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		se.Endpoint = ep
		return
	}

	if len(se.Service.Status.LoadBalancer.Ingress) == 0 || se.Service.Status.LoadBalancer.Ingress[0].IP == "" {
		return
	}
	if len(ep.Subsets) == 0 || len(ep.Subsets[0].Addresses) == 0 || ep.Subsets[0].Addresses[0].IP == "" {
		return
	}
	c.ensureRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)
	c.ServiceMap[ep.Namespace+"/"+ep.Name].Endpoint = ep
}

// Placeholder for cleanupRemovedServices
func (c *ServicesController) cleanupRemovedServices() error {
	// Placeholder logic for removing services not in the map
	return nil
}

func (c *ServicesController) ensureRules(namespace, name, svcIP, epIP string) {
	fmt.Println("ensure rules", namespace, name, svcIP, "-->", epIP)
}

func (c *ServicesController) deleteRules(namespace, name, svcIP, epIP string) {
	fmt.Println("delete rules", namespace, name, svcIP, "-->", epIP)
}
