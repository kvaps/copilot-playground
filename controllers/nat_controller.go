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
	log = ctrl.Log.WithName("nat-controller")
)

type ServiceEndpoints struct {
	Service  *v1.Service
	Endpoint *v1.Endpoints
}

type NATController struct {
	Clientset  *kubernetes.Clientset
	ServiceMap *ServiceMap
}

type ServiceMap struct {
	mu             sync.Mutex
	serviceMapping map[string]*ServiceEndpoints
}

func NewNATController(clientset *kubernetes.Clientset) *NATController {
	return &NATController{
		Clientset: clientset,
		ServiceMap: &ServiceMap{
			serviceMapping: make(map[string]*ServiceEndpoints),
		},
	}
}

func (sm *ServiceMap) Get(namespace, name string) (*ServiceEndpoints, bool) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	se, exists := sm.serviceMapping[namespace+"/"+name]
	return se, exists
}

func (sm *ServiceMap) Set(namespace, name string, se *ServiceEndpoints) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.serviceMapping[namespace+"/"+name] = se
}

func (sm *ServiceMap) Delete(namespace, name string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	delete(sm.serviceMapping, namespace+"/"+name)
}

func (sm *ServiceMap) SetEndpoint(namespace, name string, ep *v1.Endpoints) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if se, exists := sm.serviceMapping[namespace+"/"+name]; exists {
		se.Endpoint = ep
	}
}

func (c *NATController) Start(ctx context.Context) error {
	log.Info("starting nat-controller")

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
	log.Info("shutting down nat-controller")

	return nil
}

func (c *NATController) addServiceFunc(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	if !hasWholeIPAnnotation(svc) {
		return
	}

	// Fetch the corresponding endpoint if it exists
	ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		log.Error(err, "failed to get endpoints for service")
		return
	}
	if ep == nil {
		// Endpoint not found
		return
	}

	if !hasValidServiceIP(svc) || !hasValidEndpointIP(ep) {
		return
	}

	c.ensureRules(svc.Namespace, svc.Name, svc.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)

	if _, exists := c.ServiceMap.Get(svc.Namespace, svc.Name); exists {
		// Service already added
		return
	}
	c.ServiceMap.Set(svc.Namespace, svc.Name, &ServiceEndpoints{Service: svc, Endpoint: ep})
}

func (c *NATController) deleteServiceFunc(obj interface{}) {
	svc, ok := obj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	se, exists := c.ServiceMap.Get(svc.Namespace, svc.Name)
	if !exists {
		// Service already deleted
		return
	}

	if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
		return
	}

	c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
	c.ServiceMap.Delete(svc.Namespace, svc.Name)
}

func (c *NATController) updateServiceFunc(oldObj, newObj interface{}) {
	svc, ok := newObj.(*v1.Service)
	if !ok {
		// object is not Service
		return
	}
	if !hasWholeIPAnnotation(svc) {
		if se, exists := c.ServiceMap.Get(svc.Namespace, svc.Name); exists {
			if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
				return
			}
			c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
			c.ServiceMap.Delete(svc.Namespace, svc.Name)
			return
		}
	}

	if !hasWholeIPAnnotation(svc) {
		return
	}

	se, exists := c.ServiceMap.Get(svc.Namespace, svc.Name)

	// Service have no IP
	if !hasValidServiceIP(svc) {
		if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
			return
		}
		c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		c.ServiceMap.Delete(svc.Namespace, svc.Name)
		return
	}

	// Fetch the corresponding endpoint if it exists
	ep, err := c.Clientset.CoreV1().Endpoints(svc.Namespace).Get(context.TODO(), svc.Name, metav1.GetOptions{})
	if err != nil && !errors.IsNotFound(err) {
		log.Error(err, "failed to get endpoints for service")
		return
	}
	if ep == nil {
		// Endpoint not found
		return
	}

	// Endpoint have no IP
	if !hasValidEndpointIP(ep) {
		if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
			return
		}
		c.deleteRules(svc.Namespace, svc.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		c.ServiceMap.Delete(svc.Namespace, svc.Name)
		return
	}

	c.ensureRules(svc.Namespace, svc.Name, svc.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)

	if exists {
		se.Service = svc
	} else {
		c.ServiceMap.Set(svc.Namespace, svc.Name, &ServiceEndpoints{Service: svc, Endpoint: ep})
	}
	c.ServiceMap.SetEndpoint(svc.Namespace, svc.Name, ep)
}

func (c *NATController) addEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	se, exists := c.ServiceMap.Get(ep.Namespace, ep.Name)
	if !exists {
		// Service is not managed by us
		return
	}

	if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(ep) {
		return
	}

	c.ensureRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)
	c.ServiceMap.SetEndpoint(ep.Namespace, ep.Name, ep)
}

func (c *NATController) deleteEndpointFunc(obj interface{}) {
	ep, ok := obj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}

	se, exists := c.ServiceMap.Get(ep.Namespace, ep.Name)
	if !exists {
		// service is not managed by us
		return
	}

	if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
		return
	}

	c.deleteRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
	c.ServiceMap.SetEndpoint(ep.Namespace, ep.Name, nil)
}

func (c *NATController) updateEndpointFunc(oldObj, newObj interface{}) {
	ep, ok := newObj.(*v1.Endpoints)
	if !ok {
		// object is not Endpoints
		return
	}
	se, exists := c.ServiceMap.Get(ep.Namespace, ep.Name)
	if !exists {
		// service is not managed by us
		return
	}

	if !hasValidEndpointIP(ep) {
		if !hasValidServiceIP(se.Service) || !hasValidEndpointIP(se.Endpoint) {
			return
		}
		c.deleteRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, se.Endpoint.Subsets[0].Addresses[0].IP)
		c.ServiceMap.SetEndpoint(ep.Namespace, ep.Name, ep)
		return
	}

	if !hasValidServiceIP(se.Service) {
		return
	}
	if !hasValidEndpointIP(ep) {
		return
	}
	c.ensureRules(ep.Namespace, ep.Name, se.Service.Status.LoadBalancer.Ingress[0].IP, ep.Subsets[0].Addresses[0].IP)
	c.ServiceMap.SetEndpoint(ep.Namespace, ep.Name, ep)
}

func hasValidServiceIP(svc *v1.Service) bool {
	return len(svc.Status.LoadBalancer.Ingress) > 0 && svc.Status.LoadBalancer.Ingress[0].IP != ""
}

func hasValidEndpointIP(ep *v1.Endpoints) bool {
	return len(ep.Subsets) > 0 && len(ep.Subsets[0].Addresses) > 0 && ep.Subsets[0].Addresses[0].IP != ""
}

func hasWholeIPAnnotation(svc *v1.Service) bool {
	val, ok := svc.Annotations["networking.cozystack.io/wholeIP"]
	return ok && val == "true"
}

func (c *NATController) cleanupRemovedServices() error {
	// Placeholder logic for removing services not in the map
	return nil
}

func (c *NATController) ensureRules(namespace, name, svcIP, podIP string) {
	log.Info(fmt.Sprintf("ensure rules for %s/%s: %s --> %s", namespace, name, svcIP, podIP))
}

func (c *NATController) deleteRules(namespace, name, svcIP, podIP string) {
	log.Info(fmt.Sprintf("delete rules for %s/%s: %s --> %s", namespace, name, svcIP, podIP))
}
