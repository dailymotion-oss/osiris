package cmd

import (
	"context"

	"github.com/golang/glog"

	deployments "github.com/dailymotion-oss/osiris/pkg/deployments/activator"
	"github.com/dailymotion-oss/osiris/pkg/kubernetes"
	"github.com/dailymotion-oss/osiris/pkg/version"
)

func RunActivator(ctx context.Context) {
	glog.Infof(
		"Starting Osiris Activator -- version %s -- commit %s",
		version.Version(),
		version.Commit(),
	)

	client, err := kubernetes.Client()
	if err != nil {
		glog.Fatalf("Error building kubernetes clientset: %s", err)
	}

	cfg, err := deployments.GetConfigFromEnvironment()
	if err != nil {
		glog.Fatalf("Error getting activator envconfig: %s", err.Error())
	}

	// Run the activator
	deployments.NewActivator(cfg, client).Run(ctx)
}
