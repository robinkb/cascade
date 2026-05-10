package operator

import (
	"math/rand/v2"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/envtest"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	. "github.com/robinkb/cascade/testing"
)

const (
	namespaceCharset = "abcdefghijklmnopqrstuvwxyz"
)

func TestOperator(t *testing.T) {
	client := setupEnvTest(t)

	t.Run("Node Controller", func(t *testing.T) {
		testControllerNode(t, client)
	})

	t.Run("Leader Controller", func(t *testing.T) {
		testControllerLeader(t, client)
	})
}

func setupEnvTest(t *testing.T) client.Client {
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

	client, err := client.New(config, client.Options{})
	AssertNoError(t, err).Require()

	return client
}

// request generates a [reconcile.Request] for the given namespace and name.
func request(namespace, name string) reconcile.Request {
	return reconcile.Request{
		NamespacedName: types.NamespacedName{
			Namespace: namespace,
			Name:      name,
		},
	}
}

// ensureNamespace ensures that the given namespace exists.
func ensureNamespace(t *testing.T, c client.Client, namespace string) {
	ns := corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}

	err := c.Get(t.Context(), types.NamespacedName{Name: namespace}, &ns)
	if apierrors.IsNotFound(err) {
		err = c.Create(t.Context(), &ns)
	}
	AssertNoError(t, err).Require()
}

// randomNamespace generates a random namespace name and ensures that it exists.
func randomNamespace(t *testing.T, c client.Client) string {
	data := make([]byte, 32)
	for i := range data {
		data[i] = namespaceCharset[rand.IntN(len(namespaceCharset))]
	}
	namespace := string(data)

	ensureNamespace(t, c, namespace)
	return namespace
}
