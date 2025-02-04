package main

import (
	"flag"
	"os"

	_ "k8s.io/client-go/plugin/pkg/client/auth"

	"nat-controller/controllers"
	"nat-controller/nat" // Add this import

	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/kubernetes"
)

var (
	scheme = runtime.NewScheme()
	log    = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	var probeAddr string
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":0", "The address the probe endpoint binds to.")
	opts := zap.Options{
		Development: true,
	}
	opts.BindFlags(flag.CommandLine)
	flag.Parse()

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		HealthProbeBindAddress: probeAddr,
	})
	if err != nil {
		log.Error(err, "unable to start manager")
		os.Exit(1)
	}

	cfg := mgr.GetConfig()
	cfg.GroupVersion = &corev1.SchemeGroupVersion
	cfg.APIPath = "/api"

	cfg.NegotiatedSerializer = serializer.WithoutConversionCodecFactory{
		CodecFactory: serializer.NewCodecFactory(scheme),
	}

	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Error(err, "failed to create clientset")
		os.Exit(1)
	}

	// Use the DummyNATProcessor instead of the current NATProcessor
	controller := &controllers.NATController{
		Clientset: clientset,
		NAT:       &nat.NFTNATProcessor{}, // Add this line
	}

	if err := mgr.Add(controller); err != nil {
		log.Error(err, "unable to add endpoints controller to manager")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		log.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	log.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		log.Error(err, "problem running manager")
		os.Exit(1)
	}
}
