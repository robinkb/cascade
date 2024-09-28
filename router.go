package main

import (
	"fmt"
	"regexp"
	"strings"
)

func Route(path string) (string, string) {
	re := regexp.MustCompile(strings.Join([]string{
		regexp.QuoteMeta("/v2/"),
		`(?P<name>[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*)`,
		regexp.QuoteMeta("/blobs/"),
		`(?P<digest>[a-z0-9]+([+._-]|[a-z0-9]+):[a-zA-Z0-9=_-]+)`,
	}, ""))

	match := re.FindStringSubmatch(path)
	params := make([]string, 0)
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			params = append(params, fmt.Sprintf("%s=%s", name, match[i]))
		}
	}
	return "getBlobs", strings.Join(params, ",")
}
