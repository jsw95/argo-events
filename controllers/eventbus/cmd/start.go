package cmd

import (
	"fmt"

	"go.uber.org/zap"
	appv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/source"

	argoevents "github.com/argoproj/argo-events"
	"github.com/argoproj/argo-events/common"
	"github.com/argoproj/argo-events/common/logging"
	"github.com/argoproj/argo-events/controllers"
	"github.com/argoproj/argo-events/controllers/eventbus"
	eventbusv1alpha1 "github.com/argoproj/argo-events/pkg/apis/eventbus/v1alpha1"
	eventsourcev1alpha1 "github.com/argoproj/argo-events/pkg/apis/eventsource/v1alpha1"
	sensorv1alpha1 "github.com/argoproj/argo-events/pkg/apis/sensor/v1alpha1"
)

func Start(namespaced bool, managedNamespace string) {
	logger := logging.NewArgoEventsLogger().Named(eventbus.ControllerName)
	config, err := controllers.LoadConfig(func(err error) {
		logger.Errorf("Failed to reload global configuration file", zap.Error(err))
	})
	if err != nil {
		logger.Fatalw("Failed to load global configuration file", zap.Error(err))
	}
	opts := ctrl.Options{
		MetricsBindAddress:     fmt.Sprintf(":%d", common.ControllerMetricsPort),
		HealthProbeBindAddress: ":8081",
	}
	if namespaced {
		opts.Namespace = managedNamespace
	}
	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), opts)
	if err != nil {
		logger.Fatalw("unable to get a controller-runtime manager", zap.Error(err))
	}

	// Readyness probe
	if err := mgr.AddReadyzCheck("readiness", healthz.Ping); err != nil {
		logger.Fatalw("unable add a readiness check", zap.Error(err))
	}

	// Liveness probe
	if err := mgr.AddHealthzCheck("liveness", healthz.Ping); err != nil {
		logger.Fatalw("unable add a health check", zap.Error(err))
	}

	if err := eventbusv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		logger.Fatalw("unable to add scheme", zap.Error(err))
	}

	if err := eventsourcev1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		logger.Fatalw("unable to add EventSource scheme", zap.Error(err))
	}

	if err := sensorv1alpha1.AddToScheme(mgr.GetScheme()); err != nil {
		logger.Fatalw("unable to add Sensor scheme", zap.Error(err))
	}

	// A controller with DefaultControllerRateLimiter
	c, err := controller.New(eventbus.ControllerName, mgr, controller.Options{
		Reconciler: eventbus.NewReconciler(mgr.GetClient(), mgr.GetScheme(), config, logger),
	})
	if err != nil {
		logger.Fatalw("unable to set up individual controller", zap.Error(err))
	}

	// Watch EventBus and enqueue EventBus object key
	if err := c.Watch(&source.Kind{Type: &eventbusv1alpha1.EventBus{}}, &handler.EnqueueRequestForObject{},
		predicate.Or(
			predicate.GenerationChangedPredicate{},
			predicate.LabelChangedPredicate{},
		)); err != nil {
		logger.Fatalw("unable to watch EventBus", zap.Error(err))
	}

	// Watch ConfigMaps and enqueue owning EventBus key
	if err := c.Watch(&source.Kind{Type: &corev1.ConfigMap{}}, &handler.EnqueueRequestForOwner{OwnerType: &eventbusv1alpha1.EventBus{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("unable to watch ConfigMaps", zap.Error(err))
	}

	// Watch Secrets and enqueue owning EventBus key
	if err := c.Watch(&source.Kind{Type: &corev1.Secret{}}, &handler.EnqueueRequestForOwner{OwnerType: &eventbusv1alpha1.EventBus{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("unable to watch Secrets", zap.Error(err))
	}

	// Watch StatefulSets and enqueue owning EventBus key
	if err := c.Watch(&source.Kind{Type: &appv1.StatefulSet{}}, &handler.EnqueueRequestForOwner{OwnerType: &eventbusv1alpha1.EventBus{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("unable to watch StatefulSets", zap.Error(err))
	}

	// Watch Services and enqueue owning EventBus key
	if err := c.Watch(&source.Kind{Type: &corev1.Service{}}, &handler.EnqueueRequestForOwner{OwnerType: &eventbusv1alpha1.EventBus{}, IsController: true}, predicate.GenerationChangedPredicate{}); err != nil {
		logger.Fatalw("unable to watch Services", zap.Error(err))
	}

	logger.Infow("starting eventbus controller", "version", argoevents.GetVersion())
	if err := mgr.Start(signals.SetupSignalHandler()); err != nil {
		logger.Fatalw("unable to run eventbus controller", zap.Error(err))
	}
}
