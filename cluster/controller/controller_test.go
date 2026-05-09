package controller

import (
	"testing"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/envtest"

	. "github.com/robinkb/cascade/testing"
)

func TestController(t *testing.T) {
	config := setupEnvTest(t)

	t.Run("Node Reconciler", func(t *testing.T) {
		testReconcilerNode(t, config)
	})

	t.Run("Leader Reconciler", func(t *testing.T) {
		testReconcilerLeader(t, config)
	})
}

func setupEnvTest(t *testing.T) *rest.Config {
	t.Helper()

	path, err := envtest.SetupEnvtestDefaultBinaryAssetsDirectory()
	AssertNoError(t, err).Require()

	env := envtest.Environment{
		DownloadBinaryAssets:  true,
		BinaryAssetsDirectory: path,
	}

	config, err := env.Start()
	AssertNoError(t, err).Require()

	t.Cleanup(func() {
		err = env.Stop()
		AssertNoError(t, err).Require()
	})

	return config
}
