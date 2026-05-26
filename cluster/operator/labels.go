package operator

import "k8s.io/apimachinery/pkg/labels"

var commonLabels labels.Set = labels.Set{
	"app.kubernetes.io/name": "cascade-registry",
}
