package main

import "testing"

func TestRoute(t *testing.T) {
	type args struct {
		path string
	}
	type want struct {
		endpoint, params string
	}
	tests := []struct {
		name string
		args args
		want want
	}{
		{
			"get blobs for library/fedora",
			args{"/v2/library/fedora/blobs/sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"},
			want{"getBlobs", "name=library/fedora,digest=sha256:6c3c624b58dbbcd3c0dd82b4c53f04194d1247c6eebdaab7c610cf7d66709b3b"},
		},
		{
			"get blobs for containers/podman",
			args{"/v2/containers/podman/blobs/sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"},
			want{"getBlobs", "name=containers/podman,digest=sha256:090d62172504756bea09f64a28920d4f13ab6d375d436f936967f5fe4bd98a64"},
		},
		{
			"get blobs for kubernetes/cluster/network",
			args{"/v2/kubernetes/cluster/network/blobs/sha256:4a5f76c99ba8ee890053f3ec05b5387f536c51c8a34613633ffa6c2368b5bb37"},
			want{"getBlobs", "name=kubernetes/cluster/network,digest=sha256:4a5f76c99ba8ee890053f3ec05b5387f536c51c8a34613633ffa6c2368b5bb37"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotEndpoint, gotParams := Route(tt.args.path)
			if gotEndpoint != tt.want.endpoint {
				t.Errorf("Route() got endpoint = %v, want %v", gotEndpoint, tt.want.endpoint)
			}
			if gotParams != tt.want.params {
				t.Errorf("Route() got params = %v, want %v", gotParams, tt.want.params)
			}
		})
	}
}
