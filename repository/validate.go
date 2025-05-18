package repository

import (
	"fmt"
	"regexp"
)

const (
	repositoryNameRegexp = `[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*(\/[a-z0-9]+((\.|_|__|-+)[a-z0-9]+)*)*$`
	tagRegexp            = `[a-zA-Z0-9_][a-zA-Z0-9._-]{0,127}$`
)

var (
	reRepositoryName = regexp.MustCompile(fmt.Sprintf("^%s$", repositoryNameRegexp))
	reTag            = regexp.MustCompile(fmt.Sprintf("^%s$", tagRegexp))
)

func ValidateRepositoryName(name string) bool {
	return reRepositoryName.MatchString(name)
}

func ValidateTag(reference string) bool {
	return reTag.MatchString(reference)
}
