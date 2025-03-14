package cascade

import "strings"

var (
	// Error codes as defined in the Distribution specification:
	// https://github.com/opencontainers/distribution-spec/blob/main/spec.md#error-codes
	ErrBlobUnknown         = Error{Code: "BLOB_UNKNOWN", Message: "blob unknown to registry"}
	ErrBlobUploadInvalid   = Error{Code: "BLOB_UPLOAD_INVALID", Message: "blob upload invalid"}
	ErrBlobUploadUnknown   = Error{Code: "BLOB_UPLOAD_UNKNOWN", Message: "blob upload unknown to registry"}
	ErrDigestInvalid       = Error{Code: "DIGEST_INVALID", Message: "provided digest did not match uploaded content"}
	ErrManifestBlobUnknown = Error{Code: "MANIFEST_BLOB_UNKNOWN", Message: "manifest references a manifest or blob unknown to registry"}
	ErrManifestInvalid     = Error{Code: "MANIFEST_INVALID", Message: "manifest invalid"}
	ErrManifestUnknown     = Error{Code: "MANIFEST_UNKNOWN", Message: "manifest unknown to registry"}
	ErrNameInvalid         = Error{Code: "NAME_INVALID", Message: "invalid repository name"}
	ErrNameUnknown         = Error{Code: "NAME_UNKNOWN", Message: "repository name not known to registry"}
	ErrSizeInvalid         = Error{Code: "SIZE_INVALID", Message: "provided length did not match content length"}
	ErrUnauthorized        = Error{Code: "UNAUTHORIZED", Message: "authentication required"}
	ErrDenied              = Error{Code: "DENIED", Message: "requested access to the resource is denied"}
	ErrUnsupported         = Error{Code: "UNSUPPORTED", Message: "the operation is unsupported"}
	ErrTooManyRequests     = Error{Code: "TOOMANYREQUESTS", Message: "too many requests"}

	// Extra error codes not defined in the spec.
	ErrTagInvalid          = Error{Code: "TAG_INVALID", Message: "tag invalid"}
	ErrUploadOffsetInvalid = Error{Code: "UPLOAD_OFFSET_INVALID", Message: "provided upload content range is invalid"}
)

type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
	Detail  string `json:"detail,omitempty"`
}

func (e Error) Error() string {
	return e.Message
}

func NewErrorResponse(err ...Error) *ErrorResponse {
	return &ErrorResponse{
		Errors: err,
	}
}

type ErrorResponse struct {
	Errors []Error `json:"errors"`
}

func (e ErrorResponse) Error() string {
	errs := make([]string, len(e.Errors))
	for i := range e.Errors {
		errs[i] = e.Errors[i].Error()
	}

	return strings.Join(errs, ", ")
}
